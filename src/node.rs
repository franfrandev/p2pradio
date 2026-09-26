use crate::{
    AppError,
    backend::{EncodingBackend, EncodingBackendCtx},
    cli::{Listener, Streamer, Variant},
    p2p::swarm::{ListenerSwarmCtx, ListenerSwarmService, StreamerSwarmCtx, StreamerSwarmService},
    rpc::{ListenerServerService, create_listener_server},
    rpc::{StreamerServerCtx, StreamerServerService},
    utils::{
        AsyncService, Service, TaskRet, handle_loop_cannot_fail_handle, handle_to_main_handle,
    },
};
use libp2p::{PeerId, gossipsub::IdentTopic};
use std::net::SocketAddr;
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

pub struct StreamerNode {
    swarm_service: StreamerSwarmService,
    streamer_server: StreamerServerService,
    backend: EncodingBackend,
}

pub struct StreamerNodeCtx {
    topic: IdentTopic,
    url: String,
    rpc_addr: SocketAddr,
    http_addr: SocketAddr,
    cancel: CancellationToken,
}

impl AsyncService<StreamerNodeCtx, AppError, AppError> for StreamerNode {
    async fn new(ctx: StreamerNodeCtx) -> Result<Self, AppError>
    where
        Self: Sized,
    {
        let (gossip_pub_tx, gossip_pub_rx) = mpsc::channel(100);
        let swarm_service = StreamerSwarmService::new(StreamerSwarmCtx {
            topic: ctx.topic,
            gossip_pub_rx,
            cancel: ctx.cancel.clone(),
        })?;

        let streamer_server = StreamerServerService::new(StreamerServerCtx {
            rpc_addr: ctx.rpc_addr,
            http_addr: ctx.http_addr,
            peer_id: swarm_service.peer_id(),
            cancel: ctx.cancel.clone(),
        })
        .await?;

        let backend = EncodingBackend::new(EncodingBackendCtx {
            gossip_tx: gossip_pub_tx,
            url: ctx.url,
            cancel: ctx.cancel.clone(),
        })?;

        Ok(StreamerNode {
            swarm_service,
            streamer_server,
            backend,
        })
    }

    async fn run(self) -> TaskRet<AppError> {
        let swarm = tokio::spawn(async move { self.swarm_service.run().await });
        let server = tokio::spawn(async move { self.streamer_server.run().await });
        let backend = tokio::spawn(async move { self.backend.run().await });

        select! {
            res = swarm => handle_loop_cannot_fail_handle(res),
            res = server => handle_to_main_handle(res),
            res = backend => handle_loop_cannot_fail_handle(res),
        }
    }
}

pub struct ListenerNode {
    swarm_service: ListenerSwarmService,
    streamer_server: ListenerServerService,
}

pub struct ListenerNodeCtx {
    topic: IdentTopic,
    rpc_addr: SocketAddr,
    listener_addr: SocketAddr,
    streamer_peer_id: PeerId,
    cancel: CancellationToken,
}

impl AsyncService<ListenerNodeCtx, AppError, AppError> for ListenerNode {
    async fn new(ctx: ListenerNodeCtx) -> Result<Self, AppError>
    where
        Self: Sized,
    {
        let (gossip_sub_tx, gossip_sub_rx) = mpsc::channel(100);
        let swarm_service = ListenerSwarmService::new(ListenerSwarmCtx {
            topic: ctx.topic,
            gossip_sub_tx,
            streamer_peer_id: ctx.streamer_peer_id,
            cancel: ctx.cancel.clone(),
        })?;

        let streamer_server = create_listener_server(
            ctx.rpc_addr,
            ctx.listener_addr,
            swarm_service.peer_id(),
            gossip_sub_rx,
            ctx.cancel.clone(),
        )
        .await?;

        Ok(ListenerNode {
            swarm_service,
            streamer_server,
        })
    }

    async fn run(self) -> TaskRet<AppError> {
        let swarm = tokio::spawn(async move { self.swarm_service.run().await });
        let server = tokio::spawn(async move { self.streamer_server.run().await });

        select! {
            res = swarm => handle_loop_cannot_fail_handle(res),
            res = server => handle_to_main_handle(res),
        }
    }
}

pub struct Node {
    topic: IdentTopic,
    rpc_addr: SocketAddr,
    http_addr: SocketAddr,
    variant: Variant,
}

impl Node {
    pub fn new(
        topic: IdentTopic,
        rpc_addr: SocketAddr,
        http_addr: SocketAddr,
        variant: Variant,
    ) -> Result<Self, AppError> {
        Ok(Node {
            topic,
            rpc_addr,
            http_addr,
            variant,
        })
    }

    pub async fn start(self, cancel: CancellationToken) -> TaskRet<AppError> {
        match self.variant {
            Variant::Streamer(Streamer { url }) => {
                let ctx = StreamerNodeCtx {
                    topic: self.topic,
                    url,
                    rpc_addr: self.rpc_addr,
                    http_addr: self.http_addr,
                    cancel,
                };
                let node_ret = StreamerNode::new(ctx).await;
                let node = match node_ret {
                    Ok(node) => node,
                    Err(err) => return Some(Err(err)),
                };
                node.run().await
            }
            Variant::Listener(Listener { streamer_peer_id }) => {
                let ctx = ListenerNodeCtx {
                    topic: self.topic,
                    rpc_addr: self.rpc_addr,
                    listener_addr: self.http_addr,
                    streamer_peer_id,
                    cancel,
                };
                let node_ret = ListenerNode::new(ctx).await;
                let node = match node_ret {
                    Ok(node) => node,
                    Err(err) => return Some(Err(err)),
                };
                node.run().await
            }
        }
    }
}
