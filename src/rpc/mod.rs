use crate::backend::{DecodingBackend, OggHeaders};
use crate::rpc::info::InfoRpcServer;
use crate::rpc::listener::ListenerRpcServer;
use crate::rpc::streamer::StreamerRpcServer;
use bytes::Bytes;
use futures_util::StreamExt;
use http::Request;
use http_body_util::StreamBody;
use http_body_util::combinators::BoxBody;
use hyper::body::{Frame, Incoming};
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use io::{AsyncRead, AsyncWrite};
use jsonrpsee::core::RegisterMethodError;
use jsonrpsee::server::{ServerBuilder, ServerHandle};
use libp2p::PeerId;
use pin_project_lite::pin_project;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::{io, select};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, log};

mod info;
mod listener;
mod streamer;

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
        gossip_rx: flume::Receiver<Vec<u8>>,
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
        gossip_rx: flume::Receiver<Vec<u8>>,
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
        cancel: CancellationToken,
    ) -> Result<RunningServer, RpcError> {
        let (addr, handle) = match self {
            ServerVariant::Streamer { rpc_addr, peer_id } => {
                start_streamer_server(rpc_addr, peer_id).await?
            }
            ServerVariant::Listener {
                rpc_addr,
                stream_addr: listener_addr,
                peer_id,
                gossip_rx,
            } => {
                let (addr, h1, h2, handle) =
                    start_listener_server(rpc_addr, listener_addr, peer_id, gossip_rx, cancel)
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
    rpc_addr: SocketAddr,
    peer_id: PeerId,
) -> Result<(SocketAddr, ServerHandle), RpcError> {
    let server = ServerBuilder::default().build(rpc_addr).await?;
    let addr = server.local_addr()?;

    let methods = info::InfoRpcImpl { peer_id }.into_rpc();

    let server_handle = server.start(methods);

    tracing::info!("RPC server started at {}", addr);

    Ok((addr, server_handle))
}

#[instrument(skip(peer_id, gossip_rx, cancel))]
async fn start_listener_server(
    rpc_addr: SocketAddr,
    listener_addr: SocketAddr,
    peer_id: PeerId,
    gossip_rx: flume::Receiver<Vec<u8>>,
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
        start_server(listener_addr, gossip_rx, cancel).await?;
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
    gossip_rx: flume::Receiver<Vec<u8>>,
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
    let dec_tx2 = dec_tx.clone();
    let headers2 = headers.clone();
    let backend = tokio::spawn(async move {
        tracing::debug!("Starting decoding backend");
        let back = DecodingBackend::new(gossip_rx, dec_tx2, headers2);
        let res = back.run(cancel_).await;
        tracing::error!("Backend finished with result: {:?}", res);
        res
    });

    let server_listener_handle =
        tokio::spawn(async move { server_listener(listener, dec_tx, headers, cancel).await });

    Ok((server_listener_handle, backend, local_addr))
}

async fn server_listener(
    listener: TcpListener,
    dec_tx: broadcast::Sender<Vec<u8>>,
    headers: OggHeaders,
    cancel: CancellationToken,
) -> Result<(), RpcError> {
    loop {
        select! {
            Ok((stream, _)) = listener.accept() => {
                log::debug!("Accepted connection: {:?}", stream.peer_addr());
                let dec_tx = dec_tx.clone();
                let headers = headers.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);

                    if let Err(err) = http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(|req| async { response_stream(req, &dec_tx, &headers).await }),
                        )
                        .await
                    {
                        eprintln!("Error serving connection: {:?}", err);
                    }
                });
            }
            _ = cancel.cancelled() => {
                return Ok(());
            }
        }
    }
}

pub async fn response_stream(
    req: Request<Incoming>,
    dec_tx: &broadcast::Sender<Vec<u8>>,
    headers: &OggHeaders,
) -> hyper::Result<Response<BoxBody<Bytes, Infallible>>> {
    tracing::debug!("Received request: {:?}", req);
    // Snapshot the headers and subscribe under the same lock, so that header pages
    // are neither missed nor duplicated if the decoder is emitting them right now.
    let (headers, rx) = {
        let headers = headers.lock().expect("headers lock poisoned");
        (headers.clone(), dec_tx.subscribe())
    };
    let live = BroadcastStream::new(rx).filter_map(|item| async move {
        match item {
            Ok(item) => {
                tracing::trace!("Received item: {:?}", item.len());
                Some(item)
            }
            // A slow client skips pages rather than being disconnected.
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                tracing::warn!("HTTP client lagged, skipped {} pages", n);
                None
            }
        }
    });
    let stream = futures_util::stream::iter(headers)
        .chain(live)
        .map(|item| Ok(Frame::data(Bytes::from(item))));
    let stream = StreamBody::new(stream);
    let body = BoxBody::new(stream);

    // TODO keep track of icecast headers
    let res = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "audio/ogg")
        .body(body)
        .expect("Failed to build Response");

    Ok(res)
}
