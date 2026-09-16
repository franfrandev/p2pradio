use jsonrpsee::core::async_trait;
use jsonrpsee::proc_macros::rpc;

#[rpc(server, namespace = "streamer")]
pub trait StreamerRpc {
    // TODO
    #[method(name = "todo")]
    async fn todo(&self);
}

pub struct StreamerRpcImpl;

#[async_trait]
impl StreamerRpcServer for StreamerRpcImpl {
    async fn todo(&self) {}
}