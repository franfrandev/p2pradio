// The implementation of the encoding / decoding backend.
// A simple illustration of its role can be run from the terminal with:
// gst-launch-1.0 filesrc location="/home/joe/Music/Unknown_Brother.mp3" ! mpegaudioparse ! tcpserversink host=0.0.0.0 port=8080
// gst-launch-1.0 tcpclientsrc host=0.0.0.0 port=8080 ! mpegaudioparse ! appsink

mod icecast_ogg;
mod ogg_dec;

use crate::AppError;
use crate::backend::icecast_ogg::{Stream, StreamWithPipeline, new_empty_stream};
use crate::backend::ogg_dec::{DecStream, DecStreamWithPipeline, new_empty_dec_stream};
use crate::p2p::codec::{Message, Metadata};
use crate::utils::{AsyncService, Ctx, Service, TaskRet};
use anyhow::{Context, anyhow};
use gstreamer_app::gst;
use std::convert::Infallible;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing_gstreamer as tracing_gst;

pub fn init_gst() -> anyhow::Result<()> {
    // tracing_gst::integrate_spans();
    tracing_gst::integrate_events();
    // gst::log::remove_default_log_function();
    gst::init()?;
    Ok(())
}

pub struct EncodingBackend {
    gossip_tx: mpsc::Sender<Message>,
    stream_with_pipeline: Stream<StreamWithPipeline>,
    cancel: CancellationToken,
}

pub struct EncodingBackendCtx {
    pub(crate) gossip_tx: mpsc::Sender<Message>,
    pub(crate) url: String,
    pub(crate) cancel: CancellationToken,
}

impl Service<EncodingBackendCtx, AppError, AppError> for EncodingBackend {
    fn new(ctx: EncodingBackendCtx) -> Result<Self, AppError>
    where
        Self: Sized,
    {
        let url = ctx.url;
        tracing::debug!("Processing url: {}", url);
        if url.is_empty() {
            return Err(anyhow!("Url is empty").into());
        }

        init_gst()?;
        let empty_stream = new_empty_stream();
        let stream_with_pipeline = empty_stream.create_icecast_pipeline(url)?;

        Ok(Self {
            gossip_tx: ctx.gossip_tx,
            stream_with_pipeline,
            cancel: ctx.cancel,
        })
    }

    async fn run(self) -> TaskRet<AppError> {
        Some(self.process_url().await)
    }
}

impl EncodingBackend {
    async fn process_url(self) -> Result<Infallible, AppError> {
        let stream_with_pipeline = &self.stream_with_pipeline;
        let sink_rx = stream_with_pipeline.sink_rx();
        let mut title_rx = stream_with_pipeline.title_rx();

        let main_loop = tokio::spawn(async move {
            tracing::debug!("Starting encoding main loop");
            if let Err(err) = self.stream_with_pipeline.main_loop(self.cancel).await {
                tracing::error!("Error in main loop: {}", err);
            }
        });

        let msg_loop: JoinHandle<Result<(), AppError>> = tokio::spawn(async move {
            loop {
                select! {
                    Ok(buf) = sink_rx.recv_async() => {
                        tracing::trace!("Received buffer of size {}", buf.len());
                        let msg = Message::AudioPacket(buf);
                        self.gossip_tx.send(msg).await.map_err(|e| e.to_string()).map_err(AppError::Other)?;
                    }
                    Ok(()) = title_rx.changed() => {
                        let value = title_rx.borrow_and_update().clone();
                        if let Some(title) = value {
                            let msg = Message::Metadata(Metadata { title });
                            self.gossip_tx.send(msg).await.map_err(|e| e.to_string()).map_err(AppError::Other)?;
                        }
                    }
                    else => break Ok(())
                }
            }
        });

        select! {
            _ = msg_loop => {
                Err(anyhow!("Message loop encoding terminated").into())
            }
            _ = main_loop => {
                Err(anyhow!("Main loop encoding terminated").into())
            }
        }
    }
}

/// Ogg header pages of the current stream, to be sent first to every new listener.
pub type OggHeaders = Arc<Mutex<Vec<Vec<u8>>>>;

pub struct DecodingBackend {
    dec_stream_with_pipeline: DecStream<DecStreamWithPipeline>,
    cancel: CancellationToken,
}

pub struct DecodingBackendCtx {
    pub audio_rx: mpsc::Receiver<Vec<u8>>,
    pub dec_tx: broadcast::Sender<Vec<u8>>,
    pub headers: OggHeaders,
    pub cancel: CancellationToken,
}

impl Ctx for DecodingBackendCtx {}

impl AsyncService<DecodingBackendCtx, anyhow::Error, anyhow::Error> for DecodingBackend {
    async fn new(ctx: DecodingBackendCtx) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        init_gst().expect("Failed to initialize GStreamer");
        let empty_dec_stream = new_empty_dec_stream(ctx.audio_rx, ctx.dec_tx);
        let dec_stream_with_pipeline = timeout(
            Duration::from_secs(10),
            empty_dec_stream.create_opus_pipeline(ctx.headers),
        )
        .await
        .context("pipeline not created after 10s")??;
        tracing::debug!("pipeline created");

        Ok(Self {
            dec_stream_with_pipeline,
            cancel: ctx.cancel,
        })
    }

    async fn run(self) -> TaskRet<anyhow::Error> {
        let cancel = self.cancel.clone();
        self.cancel
            .run_until_cancelled(async {
                let res = self
                    .dec_stream_with_pipeline
                    .run_dec_stream(cancel)
                    .await
                    .map_err(Into::into);
                let res = match res {
                    Ok(()) => anyhow!("Pipeline finished unexpectedly"),
                    Err(e) => e,
                };
                Err(res)
            })
            .await
    }
}
