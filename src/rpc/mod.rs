use crate::backend::{DecodingBackend, OggHeaders};
use crate::p2p::codec::{Message, Metadata};
use crate::rpc::info::InfoRpcServer;
use crate::rpc::listener::ListenerRpcServer;
use anyhow::Context as _;
use axum::http::{HeaderMap, HeaderName};
use axum::{Router, body::Body, extract::State, response::IntoResponse, routing::get};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use http_body_util::StreamBody;
use http_body_util::combinators::BoxBody;
use hyper::{
    Request, Response, StatusCode, body::Frame, header::CONTENT_TYPE, server::conn::http1,
    service::service_fn,
};
use io::{AsyncRead, AsyncWrite};
use jsonrpsee::{
    core::RegisterMethodError,
    server::{ServerBuilder, ServerHandle},
};
use libp2p::PeerId;
use pin_project_lite::pin_project;
use prometheus_client::{encoding::text::encode as prometheus_encode, registry::Registry};
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::{io, io::ReadBuf, net::TcpListener, select, sync::broadcast, task::JoinHandle};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

mod info;
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

/// https://www.jsonrpc.org/specification#response_object
pub mod err {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;

    pub const fn server_error(code: i32) -> i32 {
        if code >= -32099 && code <= -32000 {
            code
        } else {
            panic!("Invalid server error code");
        }
    }

    pub const FAILED_SEND_FILE_REF: i32 = server_error(-32099);
    pub const FAILED_RECV_FILE_FEED: i32 = server_error(-32098);
    pub const FILE_ERR: i32 = server_error(-32097);
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
        gossip_rx: flume::Receiver<Message>,
    },
}

pub struct RunningServer {
    pub addr: SocketAddr,
    pub handle: ServerHandle,
}

impl ServerVariant {
    pub fn streamer(rpc_addr: SocketAddr, peer_id: PeerId) -> ServerVariant {
        ServerVariant::Streamer { rpc_addr, peer_id }
    }

    pub fn listener(
        rpc_addr: SocketAddr,
        stream_addr: SocketAddr,
        peer_id: PeerId,
        gossip_rx: flume::Receiver<Message>,
    ) -> ServerVariant {
        ServerVariant::Listener {
            rpc_addr,
            stream_addr,
            peer_id,
            gossip_rx,
        }
    }

    pub async fn start_rpc_server(
        self,
        metric_registry: Registry,
        cancel: CancellationToken,
    ) -> Result<RunningServer, RpcError> {
        let (addr, handle) = match self {
            ServerVariant::Streamer { rpc_addr, peer_id } => {
                start_streamer_server(metric_registry, rpc_addr, peer_id, cancel).await?
            }
            ServerVariant::Listener {
                rpc_addr,
                stream_addr: listener_addr,
                peer_id,
                gossip_rx,
            } => {
                let (addr, h1, h2, handle) = start_listener_server(
                    metric_registry,
                    rpc_addr,
                    listener_addr,
                    peer_id,
                    gossip_rx,
                    cancel,
                )
                .await?;
                tokio::spawn(async move {
                    select! {
                        _ = h1 => {},
                        _ = h2 => {},
                    }
                });
                (addr, handle)
            }
        };

        Ok(RunningServer { addr, handle })
    }
}

impl RunningServer {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn stopped(self, cancel: CancellationToken) {
        select! {
            _ = self.handle.stopped() => {}
            _ = cancel.cancelled() => {}
        }
    }
}

pub(crate) async fn start_streamer_server(
    metrics_registry: Registry,
    rpc_addr: SocketAddr,
    peer_id: PeerId,
    cancel: CancellationToken,
) -> Result<(SocketAddr, ServerHandle), RpcError> {
    let server = ServerBuilder::default().build(rpc_addr).await?;
    let addr = server.local_addr()?;

    let methods = info::InfoRpcImpl { peer_id }.into_rpc();

    let server_handle = server.start(methods);

    tracing::info!("RPC server started at {}", addr);

    let listener = TcpListener::bind("127.0.0.1:10231").await?;
    let server_listener_handle = tokio::spawn(async move {
        server_listener(
            listener,
            metrics_registry,
            None,
            OggHeaders::new(Mutex::new(Vec::new())),
            Arc::new(RwLock::new(String::new())),
            cancel,
        )
        .await
    });

    Ok((addr, server_handle))
}

#[instrument(skip(peer_id, gossip_rx, cancel))]
async fn start_listener_server(
    metric_registry: Registry,
    rpc_addr: SocketAddr,
    listener_addr: SocketAddr,
    peer_id: PeerId,
    gossip_rx: flume::Receiver<Message>,
    cancel: CancellationToken,
) -> Result<
    (
        SocketAddr,
        JoinHandle<Result<(), RpcError>>,
        JoinHandle<Result<(), anyhow::Error>>,
        ServerHandle,
    ),
    RpcError,
> {
    tracing::debug!("Starting JSON rpc server");
    let server = ServerBuilder::default().build(rpc_addr).await?;
    let addr = server.local_addr()?;

    let mut methods = info::InfoRpcImpl { peer_id }.into_rpc();

    let (server_listener_handle, backend, local_addr) =
        start_server(listener_addr, metric_registry, gossip_rx, cancel).await?;
    methods.merge(listener::ListenerRpcImpl { local_addr }.into_rpc())?;

    let server_handle = server.start(methods);

    tracing::info!("RPC server started at {}", addr);

    Ok((addr, server_listener_handle, backend, server_handle))
}

pin_project! {
    #[derive(Debug)]
    pub struct TokioIo<T> {
        #[pin]
        inner: T,
    }
}

impl<T> TokioIo<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn inner(self) -> T {
        self.inner
    }
}

impl<T> hyper::rt::Read for TokioIo<T>
where
    T: AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let n = unsafe {
            let mut tbuf = ReadBuf::uninit(buf.as_mut());
            match AsyncRead::poll_read(self.project().inner, cx, &mut tbuf) {
                Poll::Ready(Ok(())) => tbuf.filled().len(),
                other => return other,
            }
        };

        unsafe {
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}

impl<T> hyper::rt::Write for TokioIo<T>
where
    T: AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        AsyncWrite::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        AsyncWrite::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        AsyncWrite::poll_shutdown(self.project().inner, cx)
    }

    fn is_write_vectored(&self) -> bool {
        AsyncWrite::is_write_vectored(&self.inner)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        AsyncWrite::poll_write_vectored(self.project().inner, cx, bufs)
    }
}

impl<T> AsyncRead for TokioIo<T>
where
    T: hyper::rt::Read,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        tbuf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), io::Error>> {
        //let init = tbuf.initialized().len();
        let filled = tbuf.filled().len();
        let sub_filled = unsafe {
            let mut buf = hyper::rt::ReadBuf::uninit(tbuf.unfilled_mut());

            match hyper::rt::Read::poll_read(self.project().inner, cx, buf.unfilled()) {
                Poll::Ready(Ok(())) => buf.filled().len(),
                other => return other,
            }
        };

        let n_filled = filled + sub_filled;
        // At least sub_filled bytes had to have been initialized.
        let n_init = sub_filled;
        unsafe {
            tbuf.assume_init(n_init);
            tbuf.set_filled(n_filled);
        }

        Poll::Ready(Ok(()))
    }
}

impl<T> AsyncWrite for TokioIo<T>
where
    T: hyper::rt::Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        hyper::rt::Write::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        hyper::rt::Write::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        hyper::rt::Write::poll_shutdown(self.project().inner, cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        hyper::rt::Write::poll_write_vectored(self.project().inner, cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        hyper::rt::Write::is_write_vectored(&self.inner)
    }
}

#[instrument(skip(gossip_rx, cancel))]
async fn start_server(
    listener_addr: SocketAddr,
    metrics_registry: Registry,
    gossip_rx: flume::Receiver<Message>,
    cancel: CancellationToken,
) -> Result<
    (
        JoinHandle<Result<(), RpcError>>,
        JoinHandle<Result<(), anyhow::Error>>,
        SocketAddr,
    ),
    RpcError,
> {
    tracing::debug!("Starting rpc server");
    let listener = TcpListener::bind(listener_addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!("Listening on {}", local_addr);

    let (dec_tx, _) = broadcast::channel(100);
    let headers = OggHeaders::default();

    let cancel_ = cancel.clone();
    let (app_src_tx, app_src_rx) = flume::bounded(100);
    let title = Arc::new(RwLock::new(String::new()));

    let title2 = title.clone();
    let cancel2 = cancel.clone();
    tokio::spawn(async move {
        loop {
            select! {
                Ok(msg) = gossip_rx.recv_async() => {
                    match msg {
                        Message::AudioPacket(bytes) => {
                            app_src_tx.send_async(bytes).await.ok();
                        }
                        Message::Metadata(Metadata {title}) => {
                            tracing::warn!("Received metadata: {}", title);
                            *title2.write().unwrap() = title;
                        }
                    }
                }
                _ = cancel2.cancelled() => return
            }
        }
    });

    let dec_tx2 = dec_tx.clone();
    let headers2 = headers.clone();
    let backend = tokio::spawn(async move {
        tracing::debug!("Starting decoding backend");
        let back = DecodingBackend::new(app_src_rx, dec_tx2, headers2);
        let res = back.run(cancel_).await;
        tracing::error!("Backend finished with result: {:?}", res);
        res
    });

    let server_listener_handle = tokio::spawn(async move {
        server_listener(
            listener,
            metrics_registry,
            Some(dec_tx),
            headers,
            title,
            cancel,
        )
        .await
    });

    Ok((server_listener_handle, backend, local_addr))
}

async fn server_listener(
    listener: TcpListener,
    metrics_registry: Registry,
    dec_tx: Option<broadcast::Sender<Vec<u8>>>,
    headers: OggHeaders,
    title: Arc<RwLock<String>>,
    cancel: CancellationToken,
) -> Result<(), RpcError> {
    select! {
        _ = build_server(listener, metrics_registry, headers, dec_tx, title) => {
            tracing::warn!("Server listener exited");
            Ok(())
        },
        _ = cancel.cancelled() => {
            Ok(())
        }
    }
}

async fn build_server(
    listener: TcpListener,
    metrics_registry: Registry,
    headers: OggHeaders,
    dec_tx: Option<broadcast::Sender<Vec<u8>>>,
    title: Arc<RwLock<String>>,
) -> Result<(), io::Error> {
    let is_listener = dec_tx.is_some();

    let service = StreamService::new(headers, dec_tx, title, metrics_registry);
    let mut router = Router::new().route("/metrics", get(respond_with_metrics));

    if is_listener {
        router = router.route("/stream", get(stream_handler));
    }

    axum::serve(listener, router.with_state(service)).await?;
    Ok(())
}

const METRICS_CONTENT_TYPE: &str = "application/openmetrics-text;charset=utf-8;version=1.0.0";

type SharedRegistry = Arc<Mutex<Registry>>;

async fn respond_with_metrics(state: State<StreamService>) -> impl IntoResponse {
    let mut sink = String::new();
    let reg = state.get_reg();
    prometheus_encode(&mut sink, &reg.lock().unwrap()).unwrap();

    (StatusCode::OK, [(CONTENT_TYPE, METRICS_CONTENT_TYPE)], sink)
}

#[derive(Clone)]
pub(crate) struct StreamService {
    headers: OggHeaders,
    dec_tx: Option<broadcast::Sender<Vec<u8>>>,
    title: Arc<RwLock<String>>,
    reg: SharedRegistry,
}

impl StreamService {
    fn new(
        headers: OggHeaders,
        dec_tx: Option<broadcast::Sender<Vec<u8>>>,
        title: Arc<RwLock<String>>,
        metrics_registry: Registry,
    ) -> Self {
        Self {
            headers,
            dec_tx,
            title,
            reg: Arc::new(Mutex::new(metrics_registry)),
        }
    }

    fn get_state(&self) -> &OggHeaders {
        &self.headers
    }

    fn get_dec_tx(&self) -> broadcast::Sender<Vec<u8>> {
        self.dec_tx.clone().unwrap()
    }

    fn get_title(&self) -> Arc<RwLock<String>> {
        self.title.clone()
    }

    fn get_reg(&self) -> SharedRegistry {
        Arc::clone(&self.reg)
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
