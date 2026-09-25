use crate::{
    AppError,
    backend::EncodingBackend,
    cli,
    cli::Commands,
    p2p::swarm,
    p2p::swarm::{swarm_listener, swarm_streamer, tune_in},
    rpc::{RpcError, ServerVariant},
};
use libp2p::gossipsub::IdentTopic;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub struct Node;

impl Node {
    pub async fn run(cli: cli::Cli, cancel: CancellationToken) -> Result<JoinHandle<()>, AppError> {
        let mut swarm = swarm::create_swarm()?;
        let peer_id = *swarm.local_peer_id();

        let rpc_addr = cli.http().into_socket_addr().await?;

        let topic = cli
            .topic
            .ok_or(AppError::Other("missing --topic".to_string()))?;
        let topic = IdentTopic::new(topic);
        tune_in(&mut swarm, &topic)?;

        let handle = match cli.command {
            Commands::Streamer(cli::Streamer { url }) => {
                let (gossip_tx, gossip_rx) = mpsc::channel(1000);
                let cancel2 = cancel.clone();
                let swarm = tokio::spawn(async move {
                    swarm_streamer(swarm, topic, gossip_rx, cancel2).await;
                    tracing::debug!("swarm streamer loop stopped");
                });
                let variant = ServerVariant::streamer(rpc_addr, peer_id);
                let cancel2 = cancel.clone();
                let server = tokio::spawn(async move {
                    let running_server = variant.start_rpc_server(cancel2.clone()).await?;
                    running_server.stopped(cancel2).await;
                    Ok::<(), RpcError>(())
                });
                let backend = EncodingBackend::new(gossip_tx, url);
                let backend = tokio::spawn(async move {
                    backend.start(cancel).await;
                });
                tokio::spawn(async move {
                    select! {
                        _ = swarm => tracing::debug!("swarm streamer loop stopped"),
                        _ = server => tracing::debug!("rpc server loop stopped"),
                        _ = backend => tracing::debug!("encoding backend loop stopped"),
                    }
                })
            }
            Commands::Listener(cli::Listener {
                streamer_peer_id,
                stream_addr,
                stream_port,
            }) => {
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .add_explicit_peer(&streamer_peer_id);
                let (gossip_tx, gossip_rx) = flume::unbounded();
                let cancel2 = cancel.clone();
                let swarm = tokio::spawn(async move {
                    swarm_listener(swarm, streamer_peer_id, gossip_tx, cancel2).await;
                    tracing::debug!("swarm listener loop stopped");
                });
                let stream_addr = format!("{}:{}", stream_addr, stream_port).parse().unwrap();
                let variant = ServerVariant::listener(rpc_addr, stream_addr, peer_id, gossip_rx);
                let cancel2 = cancel.clone();
                let server = tokio::spawn(async move {
                    let running_server = variant.start_rpc_server(cancel2.clone()).await?;
                    running_server.stopped(cancel2).await;
                    Ok::<_, RpcError>(())
                });
                tokio::spawn(async move {
                    select! {
                        _ = swarm => tracing::debug!("swarm listener loop stopped"),
                        res = server => match res {
                            Ok(Ok(())) => tracing::debug!("rpc server loop stopped"),
                            Ok(Err(err)) => tracing::error!("rpc server loop stopped: {err:?}"),
                            Err(err) => tracing::error!("rpc server loop panic: {err:?}")
                        },
                    }
                })
            }
        };

        Ok(handle)
    }
}
