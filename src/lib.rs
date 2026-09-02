//! Send content-addressed data over [irpc] using [bao_tree] verified streaming.
//!
//! The sender is the RPC client. It calls the receiver with a [`Send`] (hash plus
//! [`Header`]) and then streams the byte sequence produced by
//! [`bao_tree::io::fsm::encode_ranges`].
//!
//! [`Header`] is a separate initial message. `size` is the minimum the decoder
//! needs to reconstruct the Bao tree; `ranges` is there so a later revision can
//! push a subset without changing the message type.

use std::{io, net::SocketAddr};

use anyhow::{Context, Result, bail};
use bao_tree::{
    BaoTree, BlockSize, ChunkRanges, blake3,
    io::{
        fsm::{CreateOutboard, Outboard, decode_ranges, encode_ranges_validated},
        outboard::PreOrderOutboard,
    },
};
use bytes::{Bytes, BytesMut};
use iroh_io::{AsyncStreamReader, AsyncStreamWriter};
use irpc::{
    Client, WithChannels,
    channel::{mpsc, oneshot},
    rpc::noq,
    rpc_requests,
};
use serde::{Deserialize, Serialize};
use tracing::info;

/// 16 KiB leaves, the usual default for verified streaming.
pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

/// Max bytes per irpc data message.
///
/// Matches the bao leaf size so a full leaf is one frame, while the 64-byte
/// parent hash pairs from `encode_ranges` are coalesced.
const MAX_CHUNK_BYTES: usize = BLOCK_SIZE.bytes();

/// Geometry of the encoded stream that follows a [`Send`] request.
///
/// `size` is the complete size in bytes, required to reconstruct the Bao tree.
/// `ranges` is the set of chunk ranges included in the stream. A full transfer
/// uses [`ChunkRanges::all()`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    /// Complete size in bytes.
    pub size: u64,
    /// Chunk ranges included in the following encoded stream.
    pub ranges: ChunkRanges,
}

impl Header {
    /// Header for sending all chunks of a `size`-byte payload.
    pub fn full(size: u64) -> Self {
        Self {
            size,
            ranges: ChunkRanges::all(),
        }
    }
}

/// Push content-addressed data to the receiver.
///
/// After this message, the sender streams the bao-encoded byte sequence for
/// `header.ranges`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Send {
    /// BLAKE3 hash of the complete payload.
    pub hash: [u8; 32],
    /// Size and ranges for the encoded stream that follows.
    pub header: Header,
}

/// Outcome of a [`Send`].
pub type SendResult = std::result::Result<(), String>;

#[rpc_requests(message = Message)]
#[derive(Debug, Serialize, Deserialize)]
pub enum Protocol {
    /// Sender calls the receiver with a [`Send`] and a sequence of encoded chunks.
    ///
    /// The chunks are the raw byte stream produced by
    /// [`bao_tree::io::fsm::encode_ranges`].
    #[rpc(rx = mpsc::Receiver<Bytes>, tx = oneshot::Sender<SendResult>)]
    Send(Send),
}

/// Receiver actor.
pub struct Server {
    recv: tokio::sync::mpsc::Receiver<Message>,
}

impl Server {
    /// Spawn a local receiver and return a client that can send to it.
    pub fn spawn() -> Api {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(Self { recv: rx }.run());
        Api {
            inner: Client::local(tx),
        }
    }

    async fn run(mut self) {
        while let Some(msg) = self.recv.recv().await {
            match msg {
                Message::Send(msg) => {
                    tokio::spawn(async move {
                        if let Err(e) = handle_send(msg).await {
                            tracing::warn!("send handler: {e:#}");
                        }
                    });
                }
            }
        }
    }
}

async fn handle_send(msg: WithChannels<Send, Protocol>) -> Result<()> {
    let WithChannels {
        inner, tx, mut rx, ..
    } = msg;
    match recv(&inner, &mut rx).await {
        Ok(()) => {
            info!(
                hash = %blake3::Hash::from(inner.hash),
                size = inner.header.size,
                "verified"
            );
            tx.send(Ok(())).await.ok();
            Ok(())
        }
        Err(e) => {
            tx.send(Err(e.to_string())).await.ok();
            Err(e)
        }
    }
}

async fn recv(request: &Send, rx: &mut mpsc::Receiver<Bytes>) -> Result<()> {
    let tree = BaoTree::new(request.header.size, BLOCK_SIZE);
    let mut outboard = PreOrderOutboard {
        root: blake3::Hash::from(request.hash),
        tree,
        data: BytesMut::new(),
    };
    let mut target = BytesMut::new();
    decode_ranges(
        ChunkReader {
            rx,
            buf: BytesMut::new(),
        },
        request.header.ranges.clone(),
        &mut target,
        &mut outboard,
    )
    .await
    .context("decoding bao stream")?;
    Ok(())
}

/// Client used by the sender.
#[derive(Clone)]
pub struct Api {
    inner: Client<Protocol>,
}

impl Api {
    /// Connect to a remote receiver over noq.
    pub fn connect(endpoint: noq::Endpoint, addr: SocketAddr) -> Self {
        Self {
            inner: Client::noq(endpoint, addr),
        }
    }

    /// Serve this local receiver on a noq endpoint.
    pub fn listen(
        &self,
        endpoint: noq::Endpoint,
    ) -> Result<n0_future::task::AbortOnDropHandle<()>> {
        use irpc::rpc::{RemoteService, listen};
        let Some(local) = self.inner.as_local() else {
            bail!("cannot listen on a remote client");
        };
        let handle = n0_future::task::spawn(listen(endpoint, Protocol::remote_handler(local)));
        Ok(n0_future::task::AbortOnDropHandle::new(handle))
    }

    /// Encode `data` for `ranges` and wait until the receiver has verified it
    /// against the BLAKE3 hash.
    ///
    /// Returns the hash of the complete payload.
    pub async fn send(&self, data: impl Into<Bytes>, ranges: ChunkRanges) -> Result<[u8; 32]> {
        let data = data.into();
        let outboard = PreOrderOutboard::<BytesMut>::create(data.clone(), BLOCK_SIZE)
            .await
            .context("creating outboard")?;
        let hash = *outboard.root().as_bytes();
        let request = Send {
            hash,
            header: Header {
                size: outboard.tree().size(),
                ranges: ranges.clone(),
            },
        };
        let (tx, rx) = self.inner.client_streaming(request, 16).await?;
        let mut writer = Buffered::new(ChunkWriter { tx }, MAX_CHUNK_BYTES);
        encode_ranges_validated(data, outboard, &ranges, &mut writer)
            .await
            .context("encoding ranges")?;
        writer.sync().await?;
        rx.await?
            .map_err(|e| anyhow::anyhow!("receiver rejected: {e}"))?;
        Ok(hash)
    }
}

/// Glue: each write becomes one irpc message.
struct ChunkWriter {
    tx: mpsc::Sender<Bytes>,
}

impl AsyncStreamWriter for ChunkWriter {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.tx
            .send(Bytes::copy_from_slice(data))
            .await
            .map_err(io::Error::from)
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        self.tx.send(data).await.map_err(io::Error::from)
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Combinator: coalesce writes into frames of at most `max` bytes.
struct Buffered<W> {
    inner: W,
    buf: BytesMut,
    max: usize,
}

impl<W: AsyncStreamWriter> Buffered<W> {
    fn new(inner: W, max: usize) -> Self {
        Self {
            inner,
            buf: BytesMut::with_capacity(max),
            max,
        }
    }

    async fn flush_full(&mut self) -> io::Result<()> {
        while self.buf.len() >= self.max {
            let data = self.buf.split_to(self.max).freeze();
            self.inner.write_bytes(data).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn into_inner(self) -> W {
        debug_assert!(self.buf.is_empty());
        self.inner
    }
}

impl<W: AsyncStreamWriter> AsyncStreamWriter for Buffered<W> {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.buf.extend_from_slice(data);
        self.flush_full().await
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        self.write(&data).await
    }

    async fn sync(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let data = self.buf.split().freeze();
            self.inner.write_bytes(data).await?;
        }
        self.inner.sync().await
    }
}

/// Reassembles irpc chunks into the exact reads `decode_ranges` issues.
struct ChunkReader<'a> {
    rx: &'a mut mpsc::Receiver<Bytes>,
    buf: BytesMut,
}

impl ChunkReader<'_> {
    async fn fill(&mut self, needed: usize) -> io::Result<()> {
        while self.buf.len() < needed {
            match self.rx.recv().await {
                Ok(Some(chunk)) => self.buf.extend_from_slice(&chunk),
                Ok(None) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

impl AsyncStreamReader for ChunkReader<'_> {
    async fn read_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        self.fill(len).await?;
        let n = len.min(self.buf.len());
        Ok(self.buf.split_to(n).freeze())
    }

    async fn read<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        self.fill(L).await?;
        if self.buf.len() < L {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let mut out = [0u8; L];
        out.copy_from_slice(&self.buf.split_to(L));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::{ByteRanges, io::round_up_to_chunks};

    use super::*;

    struct Frames(Vec<Bytes>);

    impl AsyncStreamWriter for Frames {
        async fn write(&mut self, data: &[u8]) -> io::Result<()> {
            self.0.push(Bytes::copy_from_slice(data));
            Ok(())
        }

        async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
            self.0.push(data);
            Ok(())
        }

        async fn sync(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn buffered_write_caps_at_max() -> io::Result<()> {
        let mut w = Buffered::new(Frames(Vec::new()), 4);
        w.write(&[1, 2, 3, 4, 5, 6, 7]).await?;
        w.write(&[8, 9]).await?;
        w.sync().await?;
        assert_eq!(
            w.into_inner().0,
            [
                Bytes::from_static(&[1, 2, 3, 4]),
                Bytes::from_static(&[5, 6, 7, 8]),
                Bytes::from_static(&[9]),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn send_full() -> Result<()> {
        let api = Server::spawn();
        let data = Bytes::from(vec![7u8; 100_000]);
        let hash = api.send(data.clone(), ChunkRanges::all()).await?;
        assert_eq!(hash, *blake3::hash(&data).as_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn send_range() -> Result<()> {
        let api = Server::spawn();
        let data = Bytes::from((0..50_000).map(|i| i as u8).collect::<Vec<_>>());
        let ranges = round_up_to_chunks(&ByteRanges::from(0..10_000));
        let hash = api.send(data.clone(), ranges).await?;
        assert_eq!(hash, *blake3::hash(&data).as_bytes());
        Ok(())
    }
}
