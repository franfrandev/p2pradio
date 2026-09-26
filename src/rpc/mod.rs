use crate::{
    AppError,
    backend::{DecodingBackend, DecodingBackendCtx, OggHeaders},
    metrics::global_registry,
    p2p::codec::{Message, Metadata},
    rpc::info::InfoRpcServer,
    utils::{
        AsyncService, Ctx, Service, TaskRet, handle_loop_cannot_fail_handle, handle_to_main_handle,
    },
};
use anyhow::Context as _;
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderName},
    response::IntoResponse,
    routing::get,
};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use http_body_util::{StreamBody, combinators::BoxBody};
use hyper::{Request, Response, StatusCode, body::Frame, header::CONTENT_TYPE};
use jsonrpsee::{
    core::RegisterMethodError,
    server::{ServerBuilder, ServerHandle},
};
use libp2p::PeerId;
use prometheus_client::encoding::text::encode as prometheus_encode;
use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
};
use thiserror::Error;
use tokio::{io, net::TcpListener, select, sync::broadcast, sync::mpsc};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

mod info;
mod json;
mod listener;

pub enum ServerType {
    Listener,
    Streamer,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error(transparent)]
    Rpc(#[from] RegisterMethodError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub enum ServerVariant {
    Streamer {
        rpc_addr: SocketAddr,
        peer_id: PeerId,
    },
    Listener {
        rpc_addr: SocketAddr,
        stream_addr: SocketAddr,
        peer_id: PeerId,
        gossip_rx: mpsc::Receiver<Message>,
    },
}

pub struct StreamerServerService {
    server_listener: AxumServerStreamerService,
    server_handle: ServerHandle,
    cancel: CancellationToken,
}

pub struct StreamerServerCtx {
    pub(crate) rpc_addr: SocketAddr,
    pub(crate) http_addr: SocketAddr,
    pub(crate) peer_id: PeerId,
    pub(crate) cancel: CancellationToken,
}

impl Ctx for StreamerServerCtx {}

impl AsyncService<StreamerServerCtx, AppError, AppError> for StreamerServerService {
    async fn new(ctx: StreamerServerCtx) -> Result<Self, AppError>
    where
        Self: Sized,
    {
        let listener = TcpListener::bind(ctx.http_addr).await?;
        let server_listener = AxumServerStreamerService::new(AxumServerStreamerCtx {
            listener,
            cancel: ctx.cancel.clone(),
        })?;

        let server_handle = start_info_server(ctx.rpc_addr, ctx.peer_id).await?;

        Ok(StreamerServerService {
            server_listener,
            server_handle,
            cancel: ctx.cancel,
        })
    }

    async fn run(self) -> TaskRet<AppError> {
        let server_listener = self.server_listener;
        let server_listener_handle = tokio::spawn(async move { server_listener.run().await });

        select! {
            _ = self.server_handle.stopped() => Some(Err(AppError::TaskExited)),
            res = server_listener_handle =>
                handle_to_main_handle(res)
            ,
            _ = self.cancel.cancelled() => None,
        }
    }
}

pub async fn create_listener_server(
    rpc_addr: SocketAddr,
    listener_addr: SocketAddr,
    peer_id: PeerId,
    gossip_sub_rx: mpsc::Receiver<Message>,
    cancel: CancellationToken,
) -> Result<ListenerServerService, AppError> {
    let listener = TcpListener::bind(listener_addr).await?;
    let listener_server = ListenerServerService::new(ListenerServerCtx {
        gossip_sub_rx,
        headers: Arc::new(Mutex::new(vec![])),
        title: Arc::new(Default::default()),
        listener,
        rpc_addr,
        peer_id,
        cancel,
    })
    .await?;

    Ok(listener_server)
}

pub struct ListenerServerService {
    decoding_backend: DecodingBackend,
    axum_server: AxumServerListenerService,
    gossip_backend: GossipBackendBridge,
    server_handle: ServerHandle,
}

pub struct ListenerServerCtx {
    pub gossip_sub_rx: mpsc::Receiver<Message>,
    pub headers: OggHeaders,
    pub title: Arc<RwLock<String>>,
    pub listener: TcpListener,
    pub rpc_addr: SocketAddr,
    pub peer_id: PeerId,
    pub cancel: CancellationToken,
}

async fn start_info_server(
    rpc_addr: SocketAddr,
    peer_id: PeerId,
) -> Result<ServerHandle, AppError> {
    let server = ServerBuilder::default().build(rpc_addr).await?;
    let addr = server.local_addr()?;

    let methods = info::InfoRpcImpl { peer_id }.into_rpc();

    let server_handle = server.start(methods);

    tracing::info!("RPC server started at {}", addr);

    Ok(server_handle)
}

impl Ctx for ListenerServerCtx {}

impl AsyncService<ListenerServerCtx, AppError, AppError> for ListenerServerService {
    async fn new(ctx: ListenerServerCtx) -> Result<Self, AppError>
    where
        Self: Sized,
    {
        let (audio_tx, audio_rx) = mpsc::channel(100);
        let (dec_tx, _) = broadcast::channel(100);

        let headers = ctx.headers;
        let decoding_backend = DecodingBackend::new(DecodingBackendCtx {
            audio_rx,
            dec_tx: dec_tx.clone(),
            headers: headers.clone(),
            cancel: ctx.cancel.clone(),
        })
        .await?;

        let axum_server = AxumServerListenerService::new(AxumServerListenerCtx {
            dec_tx,
            headers,
            title: ctx.title.clone(),
            listener: ctx.listener,
            cancel: ctx.cancel.clone(),
        })?;

        let gossip_backend = GossipBackendBridge::new(GossipBackendBridgeCtx {
            gossip_sub_rx: ctx.gossip_sub_rx,
            app_src_tx: audio_tx,
            title: ctx.title,
            cancel: ctx.cancel.clone(),
        })?;

        let server_handle = start_info_server(ctx.rpc_addr, ctx.peer_id).await?;

        Ok(Self {
            decoding_backend,
            axum_server,
            gossip_backend,
            server_handle,
        })
    }

    async fn run(self) -> TaskRet<AppError> {
        let backend = tokio::spawn(async move { self.decoding_backend.run().await });
        let axum_server = tokio::spawn(async move { self.axum_server.run().await });
        let gossip_backend_bridge = tokio::spawn(async move { self.gossip_backend.run().await });
        let server_handle = tokio::spawn(async move { self.server_handle.stopped().await });

        select! {
            res = backend => handle_to_main_handle(res),
            res = axum_server => handle_to_main_handle(res),
            res = gossip_backend_bridge => handle_loop_cannot_fail_handle(res),
            _ = server_handle => Some(Err(AppError::TaskExited)),
        }
    }
}

pub struct GossipBackendBridge {
    gossip_sub_rx: mpsc::Receiver<Message>,
    app_src_tx: mpsc::Sender<Vec<u8>>,
    title: Arc<RwLock<String>>,
    cancel: CancellationToken,
}

pub struct GossipBackendBridgeCtx {
    pub gossip_sub_rx: mpsc::Receiver<Message>,
    pub app_src_tx: mpsc::Sender<Vec<u8>>,
    pub title: Arc<RwLock<String>>,
    pub cancel: CancellationToken,
}

impl Ctx for GossipBackendBridgeCtx {}

impl Service<GossipBackendBridgeCtx, Infallible, Infallible> for GossipBackendBridge {
    fn new(ctx: GossipBackendBridgeCtx) -> Result<Self, Infallible>
    where
        Self: Sized,
    {
        Ok(GossipBackendBridge {
            gossip_sub_rx: ctx.gossip_sub_rx,
            app_src_tx: ctx.app_src_tx,
            title: ctx.title,
            cancel: ctx.cancel,
        })
    }

    async fn run(mut self) -> TaskRet<Infallible> {
        self.cancel
            .run_until_cancelled(async {
                loop {
                    if let Some(msg) = self.gossip_sub_rx.recv().await {
                        match msg {
                            Message::AudioPacket(bytes) => {
                                self.app_src_tx.send(bytes).await.ok();
                            }
                            Message::Metadata(Metadata { title }) => {
                                tracing::warn!("Received metadata: {}", title);
                                *self.title.write().unwrap() = title;
                            }
                        }
                    }
                }
            })
            .await
    }
}

pub struct AxumServerListenerService {
    listener: TcpListener,
    router: Router<StreamService>,
    service: StreamService,
    cancel: CancellationToken,
}

pub struct AxumServerListenerCtx {
    dec_tx: broadcast::Sender<Vec<u8>>,
    headers: OggHeaders,
    title: Arc<RwLock<String>>,
    listener: TcpListener,
    cancel: CancellationToken,
}

impl Ctx for AxumServerListenerCtx {}

impl Service<AxumServerListenerCtx, Infallible, Infallible> for AxumServerListenerService {
    fn new(ctx: AxumServerListenerCtx) -> Result<Self, Infallible>
    where
        Self: Sized,
    {
        let service = StreamService::new(ctx.headers, ctx.dec_tx, ctx.title);
        let mut router = metrics_router();

        router = router.route("/stream", get(stream_handler));

        Ok(AxumServerListenerService {
            listener: ctx.listener,
            router,
            service,
            cancel: ctx.cancel,
        })
    }

    async fn run(self) -> TaskRet<Infallible> {
        self.cancel
            .run_until_cancelled(async move {
                axum::serve(self.listener, self.router.with_state(self.service))
                    .await
                    .unwrap();
                unreachable!()
            })
            .await
    }
}

pub struct AxumServerStreamerService {
    listener: TcpListener,
    router: Router,
    cancel: CancellationToken,
}

pub struct AxumServerStreamerCtx {
    listener: TcpListener,
    cancel: CancellationToken,
}

impl Ctx for AxumServerStreamerCtx {}

impl Service<AxumServerStreamerCtx, Infallible, Infallible> for AxumServerStreamerService {
    fn new(ctx: AxumServerStreamerCtx) -> Result<Self, Infallible>
    where
        Self: Sized,
    {
        let router = metrics_router();

        Ok(AxumServerStreamerService {
            listener: ctx.listener,
            router,
            cancel: ctx.cancel,
        })
    }

    async fn run(self) -> TaskRet<Infallible> {
        self.cancel
            .run_until_cancelled(async move {
                axum::serve(self.listener, self.router).await.unwrap();
                unreachable!()
            })
            .await
    }
}

fn metrics_router<S>() -> Router<S>
where
    S: Clone + Sync + Send + 'static,
{
    Router::new().route("/metrics", get(respond_with_metrics))
}

const METRICS_CONTENT_TYPE: &str = "application/openmetrics-text;charset=utf-8;version=1.0.0";

async fn respond_with_metrics() -> impl IntoResponse {
    let mut sink = String::new();
    let reg = global_registry();
    prometheus_encode(&mut sink, &reg.lock().unwrap()).unwrap();

    (StatusCode::OK, [(CONTENT_TYPE, METRICS_CONTENT_TYPE)], sink)
}

#[derive(Clone)]
pub(crate) struct StreamService {
    headers: OggHeaders,
    dec_tx: broadcast::Sender<Vec<u8>>,
    title: Arc<RwLock<String>>,
}

impl StreamService {
    fn new(
        headers: OggHeaders,
        dec_tx: broadcast::Sender<Vec<u8>>,
        title: Arc<RwLock<String>>,
    ) -> Self {
        Self {
            headers,
            dec_tx,
            title,
        }
    }

    fn get_state(&self) -> &OggHeaders {
        &self.headers
    }

    fn get_dec_tx(&self) -> broadcast::Sender<Vec<u8>> {
        self.dec_tx.clone()
    }

    fn get_title(&self) -> Arc<RwLock<String>> {
        self.title.clone()
    }
}

async fn stream_handler(
    state: State<StreamService>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let icy_requested = req.headers().get("Icy-MetaData").is_some_and(|v| v == "1");
    // The muxer holds this same lock while caching and broadcasting headers.
    // Subscribe atomically with the snapshot to avoid missing or duplicating them.
    let headers = state.get_state();
    let dec_tx = state.get_dec_tx();
    let (headers, rx) = {
        let headers = headers.lock().expect("headers lock poisoned");
        (headers.clone(), dec_tx.subscribe())
    };
    let initial = futures_util::stream::iter(headers.into_iter().map(Ok));
    let live = BroadcastStream::new(rx)
        .map(|item| item.context("Audio receiver lagged; reconnect to restart the stream"));
    let mut icy = IcyEncoder::new(ICY_METAINT);
    let title = state.get_title();
    let stream = initial.chain(live).map(move |item| {
        item.map(|bytes| {
            let bytes = if icy_requested {
                icy.encode(&bytes, &title.read().expect("title lock poisoned"))
            } else {
                Bytes::from(bytes)
            };
            Frame::data(bytes)
        })
    });
    let body = Body::new(StreamBody::new(stream));

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, "audio/ogg".parse().unwrap());

    if icy_requested {
        headers.insert(
            HeaderName::from_static("icy-metaint"),
            ICY_METAINT.to_string().parse().unwrap(),
        );
    }

    (StatusCode::OK, headers, body)
}

const ICY_METAINT: usize = 8192;

fn format_title(title: &str) -> Vec<u8> {
    // The length byte counts 16-byte blocks, with a maximum of 255 blocks.
    const MAX_TEXT: usize = 255 * 16;
    let mut text = String::from("StreamTitle='");
    for ch in title.chars() {
        // Avoid terminating the quoted value or injecting control characters.
        let ch = if ch == '\'' || ch.is_control() {
            ' '
        } else {
            ch
        };
        if text.len() + ch.len_utf8() + 2 > MAX_TEXT {
            break;
        }
        text.push(ch);
    }
    text.push_str("';");
    let blocks = text.len().div_ceil(16);
    let mut bytes = Vec::with_capacity(1 + blocks * 16);
    bytes.push(blocks as u8);
    bytes.extend_from_slice(text.as_bytes());
    bytes.resize(1 + blocks * 16, 0);
    bytes
}

pub async fn response_stream<B>(
    req: Request<B>,
    dec_tx: broadcast::Sender<Vec<u8>>,
    headers: &OggHeaders,
    title: Arc<RwLock<String>>,
) -> hyper::Result<Response<BoxBody<Bytes, anyhow::Error>>> {
    let icy_requested = req
        .headers()
        .get("icy-metadata")
        .is_some_and(|value| value == "1");
    // The muxer holds this same lock while caching and broadcasting headers.
    // Subscribe atomically with the snapshot to avoid missing or duplicating them.
    let (headers, rx) = {
        let headers = headers.lock().expect("headers lock poisoned");
        (headers.clone(), dec_tx.subscribe())
    };
    let initial = futures_util::stream::iter(headers.into_iter().map(Ok));
    let live = BroadcastStream::new(rx)
        .map(|item| item.context("Audio receiver lagged; reconnect to restart the stream"));
    let mut icy = IcyEncoder::new(ICY_METAINT);
    let stream = initial.chain(live).map(move |item| {
        item.map(|bytes| {
            let bytes = if icy_requested {
                icy.encode(&bytes, &title.read().expect("title lock poisoned"))
            } else {
                Bytes::from(bytes)
            };
            Frame::data(bytes)
        })
    });
    let body = BoxBody::new(StreamBody::new(stream));

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "audio/ogg");
    if icy_requested {
        response = response.header("icy-metaint", ICY_METAINT);
    }
    Ok(response.body(body).expect("Failed to build Response"))
}

struct IcyEncoder {
    interval: usize,
    remaining: usize,
}

impl IcyEncoder {
    fn new(interval: usize) -> Self {
        assert!(interval > 0);
        Self {
            interval,
            remaining: interval,
        }
    }

    fn encode(&mut self, mut audio: &[u8], title: &str) -> Bytes {
        let mut output = BytesMut::new();
        while !audio.is_empty() {
            let count = self.remaining.min(audio.len());
            output.extend_from_slice(&audio[..count]);
            audio = &audio[count..];
            self.remaining -= count;
            if self.remaining == 0 {
                output.extend_from_slice(&format_title(title));
                self.remaining = self.interval;
            }
        }
        output.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    fn strip_icy(wire: &[u8], interval: usize) -> (Vec<u8>, Vec<String>) {
        let mut rest = wire;
        let mut audio = Vec::new();
        let mut titles = Vec::new();
        while rest.len() >= interval {
            audio.extend_from_slice(&rest[..interval]);
            rest = &rest[interval..];
            let size = usize::from(rest[0]) * 16;
            let metadata = &rest[1..1 + size];
            titles.push(
                String::from_utf8(metadata.to_vec())
                    .unwrap()
                    .trim_end_matches('\0')
                    .to_owned(),
            );
            rest = &rest[1 + size..];
        }
        audio.extend_from_slice(rest);
        (audio, titles)
    }

    #[test]
    fn metadata_length_matches_padding_and_preserves_utf8() {
        for title in [String::new(), "abc".into(), "é🎵".repeat(2000)] {
            let block = format_title(&title);
            assert_eq!(block.len(), 1 + usize::from(block[0]) * 16);
            let text = std::str::from_utf8(&block[1..])
                .unwrap()
                .trim_end_matches('\0');
            assert!(text.starts_with("StreamTitle='"));
            assert!(text.ends_with("';"));
        }
    }

    #[test]
    fn icy_intervals_ignore_chunk_boundaries_and_metadata_bytes() {
        let audio: Vec<u8> = (0..101).collect();
        for chunk_size in [1, 7, 8, 9, 101] {
            let mut encoder = IcyEncoder::new(8);
            let mut wire = Vec::new();
            for chunk in audio.chunks(chunk_size) {
                wire.extend_from_slice(&encoder.encode(chunk, "Artist - Song"));
            }
            let (decoded, titles) = strip_icy(&wire, 8);
            assert_eq!(decoded, audio);
            assert_eq!(titles, vec!["StreamTitle='Artist - Song';"; 12]);
        }
        let mut encoder = IcyEncoder::new(8);
        let mut wire = encoder.encode(&audio[..8], "First").to_vec();
        wire.extend_from_slice(&encoder.encode(&audio[8..16], "Second"));
        assert_eq!(
            strip_icy(&wire, 8).1,
            ["StreamTitle='First';", "StreamTitle='Second';"]
        );
    }

    #[tokio::test]
    async fn http_replays_headers_and_negotiates_icy() {
        // Header-sized fixtures plus live data crossing several metadata intervals.
        let cached = vec![b"OggS OpusHead".to_vec(), b"OggS OpusTags".to_vec()];
        let headers = OggHeaders::default();
        *headers.lock().unwrap() = cached.clone();
        let audio = vec![42; ICY_METAINT * 3];
        for request_value in [None, Some("0"), Some("1")] {
            let (tx, _) = broadcast::channel(8);
            let mut request = Request::builder();
            if let Some(value) = request_value {
                request = request.header("Icy-MetaData", value);
            }
            let response = response_stream(
                request.body(()).unwrap(),
                tx.clone(),
                &headers,
                Arc::new(RwLock::new("Current song".into())),
            )
            .await
            .unwrap();
            assert_eq!(
                response.headers().contains_key("icy-metaint"),
                request_value == Some("1")
            );
            tx.send(audio.clone()).unwrap();
            drop(tx);
            let wire = response.into_body().collect().await.unwrap().to_bytes();
            let expected = [cached.concat(), audio.clone()].concat();
            if request_value == Some("1") {
                let (decoded, titles) = strip_icy(&wire, ICY_METAINT);
                assert_eq!(decoded, expected);
                assert_eq!(titles, vec!["StreamTitle='Current song';"; 3]);
            } else {
                assert_eq!(wire.as_ref(), expected);
            }
        }
    }
}
