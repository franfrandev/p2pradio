use crate::backend::EncodingBackend;
use crate::cli::Commands;
use crate::p2p::swarm;
use crate::p2p::swarm::{swarm_loop, tune_in};
use crate::rpc::{ServerType, start_rpc_server};
use crate::{AppError, cli};
use libp2p::gossipsub::IdentTopic;
use std::net::SocketAddr;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct Node {
    pub rpc_addr: SocketAddr,

    swarm: JoinHandle<()>,
    rpc: JoinHandle<Result<(), AppError>>,
    backend: JoinHandle<()>,
    cancel: CancellationToken,
}

impl Node {
    pub async fn run(cli: cli::Cli, cancel: CancellationToken) -> Result<Node, AppError> {
        let mut swarm = swarm::create_swarm()?;
        let peer_id = *swarm.local_peer_id();
        let (data_tx, data_rx) = mpsc::channel(1000);
        let (gossip_tx, gossip_rx) = if matches!(cli.command, Commands::Listener(..)) {
            let (tx, rx) = flume::unbounded();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let swarm = {
            let cancel = cancel.clone();
            let topic = IdentTopic::new(cli.topic.to_string());
            tune_in(&mut swarm, &topic)?;
            tokio::spawn(async move {
                swarm_loop(swarm, topic, data_rx, gossip_tx, cancel).await;
                tracing::debug!("swarm loop stopped");
            })
        };

        let (tx, rx) = oneshot::channel();
        let (rpc, maybe_file_rx) = {
            let (server_type, maybe_file_ch) = match cli.command {
                Commands::Listener(..) => (ServerType::Listener, None),
                Commands::Streamer(..) => {
                    // TODO this is not pretty
                    let (file_tx, file_rx) = mpsc::channel(1000);
                    (ServerType::Streamer, Some((file_tx, file_rx)))
                }
            };
            let addr = cli.http().into_socket_addr().await?;
            tracing::debug!("resolved socket addr: {}", addr);

            let (maybe_file_tx, maybe_file_rx) = maybe_file_ch
                .map(|(tx, rx)| (Some(tx), Some(rx)))
                .unwrap_or((None, None));

            let cancel_ = cancel.clone();
            let rpc = tokio::spawn(async move {
                let (addr, server_handle) = start_rpc_server(
                    server_type,
                    addr,
                    peer_id,
                    maybe_file_tx,
                    gossip_rx,
                    cancel_,
                )
                .await?;
                tx.send(addr).expect("unreachable");
                server_handle.stopped().await;
                tracing::debug!("rpc server stopped");
                Ok::<(), AppError>(())
            });

            (rpc, maybe_file_rx)
        };
        let rpc_addr = rx
            .await
            .map_err(|_| AppError::Other("failed to start rpc server".to_string()))?;

        let backend = {
            let backend = EncodingBackend::new(data_tx, maybe_file_rx);

            let cancel = cancel.clone();
            tokio::spawn(async move {
                backend.start(cancel).await;
            })
        };

        let node = Node {
            swarm,
            rpc,
            backend,
            rpc_addr,
            cancel,
        };

        Ok(node)
    }

    pub async fn stopped(self) -> Result<(), AppError> {
        select! {
            res = self.swarm => if let Err(err) = res {
                tracing::error!("swarm task panic: {}", err);
            },
            tres = self.rpc => {
                match tres {
                    Ok(res) => {
                        res?;
                        tracing::debug!("rpc task finished");
                    }
                    Err(err) => tracing::error!("rpc task panic: {}", err),
                }
            },
            res = self.backend => if let Err(err) = res {
                tracing::error!("backend task panic: {}", err);
            },
            _ = self.cancel.cancelled() => (),
        }

        Ok(())
    }
}
