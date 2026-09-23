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
    #[method(name = "broadcastIcecast")]
    async fn broadcast_icecast(&self, url: String) -> RpcResult<()>;
}

pub struct StreamerRpcImpl {
    pub url_tx: mpsc::Sender<(String, oneshot::Sender<Result<(), String>>)>,
}

#[async_trait]
impl StreamerRpcServer for StreamerRpcImpl {
    async fn broadcast_icecast(&self, url: String) -> RpcResult<()> {
        let span = debug_span!("broadcast_icecast", url);
        async move {
            tracing::debug!("starting icecast broadcast");
            let (tx, rx) = oneshot::channel();
            self.url_tx.send((url, tx)).await.map_err(|_| {
                ErrorObject::owned::<()>(
                    rpc_err::FAILED_SEND_FILE_REF,
                    "failed to send icecast broadcast",
                    None,
                )
            })?;
            let feedback = rx.await.map_err(|_| {
                ErrorObject::owned::<()>(
                    rpc_err::FAILED_RECV_FILE_FEED,
                    "failed to receive icecast feedback",
                    None,
                )
            })?;
            feedback.map_err(|err| {
                ErrorObject::owned(
                    rpc_err::FILE_ERR,
                    "failed to send icecast broadcast",
                    Some(err),
                )
            })?;
            Ok(())
        }
        .instrument(span)
        .await
    }
}
