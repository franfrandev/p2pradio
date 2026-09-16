use jsonrpsee::core::{RpcResult, async_trait};
use jsonrpsee::proc_macros::rpc;
use tokio::sync::mpsc;

#[rpc(server, client, namespace = "listener")]
pub trait ListenerRpc {
    #[method(name = "subscribe")]
    async fn subscribe(&self, topic: String) -> RpcResult<()>;

    #[method(name = "unsubscribe")]
    async fn unsubscribe(&self, topic: String) -> RpcResult<()>;

    #[method(name = "listen")]
    async fn listen(&self) -> RpcResult<()>;
}

pub struct ListenerRpcImpl;

#[async_trait]
impl ListenerRpcServer for ListenerRpcImpl {
    async fn subscribe(&self, topic: String) -> RpcResult<()> {
        unimplemented!()
    }

    async fn unsubscribe(&self, topic: String) -> RpcResult<()> {
        unimplemented!()
    }

    async fn listen(&self) -> RpcResult<()> {
        unimplemented!()
    }
}
