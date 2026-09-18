use crate::rpc::err as rpc_err;
use jsonrpsee::{
    core::{RpcResult, async_trait},
    proc_macros::rpc,
    types::ErrorObject,
};
use tokio::{sync::mpsc, sync::oneshot};
use tracing::{Instrument, debug_span};

#[rpc(server, client, namespace = "streamer")]
pub trait StreamerRpc {
    #[method(name = "broadcastFile")]
    async fn broadcast_file(&self, path: String) -> RpcResult<()>;
}

pub struct StreamerRpcImpl {
    pub file_tx: mpsc::Sender<(String, oneshot::Sender<Result<(), String>>)>,
}

#[async_trait]
impl StreamerRpcServer for StreamerRpcImpl {
    async fn broadcast_file(&self, path: String) -> RpcResult<()> {
        let span = debug_span!("broadcast_file", path);
        async move {
            tracing::debug!("starting broadcast");
            let (tx, rx) = oneshot::channel();
            self.file_tx.send((path, tx)).await.map_err(|_| {
                ErrorObject::owned::<()>(
                    rpc_err::FAILED_SEND_FILE_REF,
                    "failed to send file ref",
                    None,
                )
            })?;
            let feedback = rx.await.map_err(|_| {
                ErrorObject::owned::<()>(
                    rpc_err::FAILED_RECV_FILE_FEED,
                    "failed to receive file feedback",
                    None,
                )
            })?;
            feedback.map_err(|err| {
                ErrorObject::owned(rpc_err::FILE_ERR, "failed to buffer file", Some(err))
            })?;
            tracing::debug!("broadcast complete");
            Ok(())
        }
        .instrument(span)
        .await
    }
}
