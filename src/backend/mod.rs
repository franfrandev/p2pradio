// The implementation of the encoding / decoding backend.
// A simple illustration of its role can be run from the terminal with:
// gst-launch-1.0 filesrc location="/home/joe/Music/Unknown_Brother.mp3" ! mpegaudioparse ! tcpserversink host=0.0.0.0 port=8080
// gst-launch-1.0 tcpclientsrc host=0.0.0.0 port=8080 ! mpegaudioparse ! appsink

mod icecast_ogg;
mod ogg_dec;

use crate::backend::icecast_ogg::new_empty_stream;
use crate::backend::ogg_dec::new_empty_dec_stream;
use crate::p2p::codec::{Message, Metadata};
use anyhow::Context;
use gstreamer_app::gst;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
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
    url: String,
}

impl EncodingBackend {
    pub fn new(gossip_tx: mpsc::Sender<Message>, url: String) -> Self {
        Self { gossip_tx, url }
    }

    pub async fn start(self, cancel: CancellationToken) {
        init_gst().expect("Failed to initialize GStreamer");
        loop {
            select! {
                Err(err) = self.process_url(cancel.clone()) => {
                    tracing::error!("Error processing url: {}", err)
                }
                _ = cancel.cancelled() => break,
            }
        }
    }

    async fn process_url(&self, cancel: CancellationToken) -> Result<(), String> {
        let url = self.url.clone();
        tracing::debug!("Processing url: {}", url);
        if url.is_empty() {
            return Err("Url is empty".to_string());
        }

        let empty_stream = new_empty_stream();
        let stream_with_pipeline = empty_stream
            .create_icecast_pipeline(url)
            .map_err(|e| e.to_string())?;

        let sink_rx = stream_with_pipeline.sink_rx();
        let mut title_rx = stream_with_pipeline.title_rx();

        tokio::spawn(async move {
            tracing::debug!("Starting encoding main loop");
            if let Err(err) = stream_with_pipeline.main_loop(cancel).await {
                tracing::error!("Error in main loop: {}", err);
            }
        });

        loop {
            select! {
                Ok(buf) = sink_rx.recv_async() => {
                    tracing::trace!("Received buffer of size {}", buf.len());
                    let msg = Message::AudioPacket(buf);
                    self.gossip_tx.send(msg).await.map_err(|e| e.to_string())?;
                }
                Ok(()) = title_rx.changed() => {
                    let value = title_rx.borrow_and_update().clone();
                    if let Some(title) = value {
                        let msg = Message::Metadata(Metadata { title });
                        self.gossip_tx.send(msg).await.map_err(|e| e.to_string())?;
                    }
                }
            }
        }
    }
}

/// Ogg header pages of the current stream, to be sent first to every new listener.
pub type OggHeaders = Arc<Mutex<Vec<Vec<u8>>>>;

pub struct DecodingBackend {
    gossip_rx: flume::Receiver<Vec<u8>>,
    dec_tx: broadcast::Sender<Vec<u8>>,
    headers: OggHeaders,
}

impl DecodingBackend {
    pub fn new(
        gossip_rx: flume::Receiver<Vec<u8>>,
        dec_tx: broadcast::Sender<Vec<u8>>,
        headers: OggHeaders,
    ) -> Self {
        Self {
            gossip_rx,
            dec_tx,
            headers,
        }
    }

    pub async fn run(self, cancel: CancellationToken) -> Result<(), anyhow::Error> {
        init_gst()?;
        let empty_dec_stream = new_empty_dec_stream(self.gossip_rx, self.dec_tx);
        let dec_stream_with_pipeline =
            // timeout(Duration::from_secs(10), empty_dec_stream.create_pipeline())
            timeout(Duration::from_secs(10), empty_dec_stream.create_opus_pipeline(self.headers))
                .await
                .context("pipeline not created after 10s")??;
        tracing::debug!("pipeline created");
        dec_stream_with_pipeline.run_dec_stream(cancel).await
    }
}
