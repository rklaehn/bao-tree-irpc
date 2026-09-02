use std::net::{Ipv4Addr, SocketAddrV4};

use anyhow::Result;
use bao_tree::{ByteRanges, ChunkRanges, blake3, io::round_up_to_chunks};
use bao_tree_irpc::{Api, Server};
use bytes::Bytes;
use irpc::util::{make_client_endpoint, make_server_endpoint};
use n0_future::task::AbortOnDropHandle;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("Local");
    run(Server::spawn()).await?;

    println!("Remote");
    let (api, _handle) = remote_api()?;
    run(api).await?;
    Ok(())
}

async fn run(api: Api) -> Result<()> {
    let data = Bytes::from_static(b"hello, bao-tree over irpc");
    let hash = api.send(data.clone(), ChunkRanges::all()).await?;
    println!("  full  {}  {} bytes", blake3::Hash::from(hash), data.len());

    let data = Bytes::from((0..80_000).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    let ranges = round_up_to_chunks(&ByteRanges::from(0..12_345));
    let hash = api.send(data.clone(), ranges).await?;
    println!(
        "  range {}  {} bytes (sent first 12345)",
        blake3::Hash::from(hash),
        data.len()
    );
    Ok(())
}

fn remote_api() -> Result<(Api, AbortOnDropHandle<()>)> {
    let port = 18765;
    let (server, cert) =
        make_server_endpoint(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())?;
    let client =
        make_client_endpoint(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into(), &[&cert])?;
    let api = Server::spawn();
    let handle = api.listen(server)?;
    let remote = Api::connect(client, SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).into());
    Ok((remote, handle))
}
