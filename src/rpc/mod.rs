use crate::rpc::info::InfoRpcServer;
use crate::rpc::listener::ListenerRpcServer;
use crate::rpc::streamer::StreamerRpcServer;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http_body_util::StreamBody;
use http_body_util::combinators::BoxBody;
use hyper::body::{Frame, Incoming};
use hyper::header::CONTENT_TYPE;
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use jsonrpsee::core::{RegisterMethodError, Serialize};
use jsonrpsee::server::{ServerBuilder, ServerHandle, serve};
use libp2p::PeerId;
use std::convert::Infallible;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::io;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

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

pub async fn start_rpc_server(
    server_type: ServerType,
    address: SocketAddr,
    peer_id: PeerId,
    file_tx: Option<mpsc::Sender<(String, oneshot::Sender<Result<(), String>>)>>,
    gossip_rx: Option<flume::Receiver<Vec<u8>>>,
    cancel: CancellationToken,
) -> Result<(SocketAddr, ServerHandle), RpcError> {
    let server = ServerBuilder::default().build(address).await?;
    let addr = server.local_addr()?;

    let mut methods = info::InfoRpcImpl { peer_id }.into_rpc();

    match server_type {
        ServerType::Listener => {
            let gossip_rx = gossip_rx.expect("should be defined for listener");
            let (_handle, local_addr) = start_server(gossip_rx, cancel).await?;
            methods.merge(listener::ListenerRpcImpl { local_addr }.into_rpc())?
        }
        ServerType::Streamer => methods.merge(
            streamer::StreamerRpcImpl {
                file_tx: file_tx.expect("file_tx must be provided for streamer server"),
            }
            .into_rpc(),
        )?,
    };

    let server_handle = server.start(methods);

    tracing::info!("RPC server started at {}", addr);

    Ok((addr, server_handle))
}

use crate::backend::DecodingBackend;
use http::Request;
use hyper::rt::{Sleep, Timer};
use hyper::server::conn::http1;
use pin_project_lite::pin_project;
use std::sync::{Arc, Mutex};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::log;

#[derive(Clone)]
/// An Executor that uses the tokio runtime.
pub struct TokioExecutor;

impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn(fut);
    }
}

/// A Timer that uses the tokio runtime.

#[derive(Clone, Debug)]
pub struct TokioTimer;

impl Timer for TokioTimer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep(duration),
        })
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep_until(deadline.into()),
        })
    }

    fn now(&self) -> Instant {
        tokio::time::Instant::now().into()
    }

    fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
        if let Some(sleep) = sleep.as_mut().downcast_mut_pin::<TokioSleep>() {
            sleep.reset(new_deadline)
        }
    }
}

impl TokioTimer {
    /// Create a new TokioTimer
    pub fn new() -> Self {
        Self {}
    }
}

// Use TokioSleep to get tokio::time::Sleep to implement Unpin.
// see https://docs.rs/tokio/latest/tokio/time/struct.Sleep.html
pin_project! {
    pub(crate) struct TokioSleep {
        #[pin]
        pub(crate) inner: tokio::time::Sleep,
    }
}

impl Future for TokioSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

impl Sleep for TokioSleep {}

impl TokioSleep {
    pub fn reset(self: Pin<&mut Self>, deadline: Instant) {
        self.project().inner.as_mut().reset(deadline.into());
    }
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
    T: tokio::io::AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let n = unsafe {
            let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
            match tokio::io::AsyncRead::poll_read(self.project().inner, cx, &mut tbuf) {
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
    T: tokio::io::AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_shutdown(self.project().inner, cx)
    }

    fn is_write_vectored(&self) -> bool {
        tokio::io::AsyncWrite::is_write_vectored(&self.inner)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write_vectored(self.project().inner, cx, bufs)
    }
}

impl<T> tokio::io::AsyncRead for TokioIo<T>
where
    T: hyper::rt::Read,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        tbuf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
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

impl<T> io::AsyncWrite for TokioIo<T>
where
    T: hyper::rt::Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        hyper::rt::Write::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        hyper::rt::Write::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
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

async fn start_server(
    gossip_rx: flume::Receiver<Vec<u8>>,
    cancel: CancellationToken,
) -> Result<(JoinHandle<Result<(), RpcError>>, SocketAddr), RpcError> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));

    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!("Listening on {}", local_addr);

    let (stream_tx, stream_rx) = flume::unbounded();

    let cancel_ = cancel.clone();
    let backend = tokio::spawn(async move {
        tracing::debug!("Starting decoding backend");
        let back = DecodingBackend::new(gossip_rx, stream_tx);
        let res = back.run(cancel_).await;
        tracing::error!("Backend finished with result: {:?}", res);
        res
    });

    let handle = tokio::spawn(async move { server_listener(listener, stream_rx.clone()).await });

    Ok((handle, local_addr))
}

async fn server_listener(
    listener: TcpListener,
    stream_rx: flume::Receiver<Vec<u8>>,
) -> Result<(), RpcError> {
    loop {
        let (stream, _) = listener.accept().await?;
        log::debug!("Accepted connection: {:?}", stream.peer_addr());
        let stream_rx = stream_rx.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);

            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(|req| async { response_stream(req, stream_rx.clone()).await }),
                )
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}

pub async fn response_stream(
    req: Request<Incoming>,
    stream_rx: flume::Receiver<Vec<u8>>,
) -> hyper::Result<Response<BoxBody<Bytes, Infallible>>> {
    tracing::debug!("Received request: {:?}", req);
    let stream = stream_rx.into_stream();
    let stream = stream.map(|item| {
        tracing::trace!("Received item: {:?}", item.len());
        let b = Bytes::from(item);
        let f = Frame::data(b);
        Ok(f)
    });
    let stream = StreamBody::new(stream);
    let body = BoxBody::new(stream);

    let res = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "audio/mpeg")
        .body(body)
        .expect("Failed to build Response");

    Ok(res)
}
