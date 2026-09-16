use crate::rpc::listener::ListenerRpcServer;
use crate::rpc::streamer::StreamerRpcServer;
use jsonrpsee::server::{ServerBuilder, ServerHandle};
use std::net::SocketAddr;
use tokio::io;

mod listener;
mod streamer;

pub enum ServerType {
    Listener,
    Streamer,
}

pub async fn start_rpc_server(
    server_type: ServerType,
    address: SocketAddr,
) -> io::Result<(SocketAddr, ServerHandle)> {
    let server = ServerBuilder::default().build(address).await?;
    let addr = server.local_addr()?;

    let server_handle = match server_type {
        ServerType::Listener => server.start(listener::ListenerRpcImpl.into_rpc()),
        ServerType::Streamer => server.start(streamer::StreamerRpcImpl.into_rpc()),
    };

    tracing::info!("RPC server started at {}", addr);

    Ok((addr, server_handle))
}
