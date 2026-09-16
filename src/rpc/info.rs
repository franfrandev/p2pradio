use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use libp2p::PeerId;

#[rpc(server, client, namespace = "info")]
pub trait InfoRpc {
    #[method(name = "peerId")]
    fn peer_id(&self) -> RpcResult<String>;
}

pub struct InfoRpcImpl {
    pub(crate) peer_id: PeerId,
}

impl InfoRpcServer for InfoRpcImpl {
    fn peer_id(&self) -> RpcResult<String> {
        Ok(self.peer_id.to_base58())
    }
}
