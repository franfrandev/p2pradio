use jsonrpsee::core::{RpcResult, async_trait};
use jsonrpsee::proc_macros::rpc;
use std::net::SocketAddr;

#[rpc(server, client, namespace = "listener")]
pub trait ListenerRpc {
    #[method(name = "localAddr")]
    async fn local_addr(&self) -> RpcResult<SocketAddr>;
}

pub struct ListenerRpcImpl {
    pub local_addr: SocketAddr,
}

#[async_trait]
impl ListenerRpcServer for ListenerRpcImpl {
    async fn local_addr(&self) -> RpcResult<SocketAddr> {
        Ok(self.local_addr)
    }
}
