// The implementation of the encoding / decoding backend.
// A simple illustration of its role can be run from the terminal with:
// gst-launch-1.0 filesrc location="/home/joe/Music/Unknown_Brother.mp3" ! mpegaudioparse ! tcpserversink host=0.0.0.0 port=8080
// gst-launch-1.0 tcpclientsrc host=0.0.0.0 port=8080 ! mpegaudioparse ! appsink

pub mod mpeg_audio_parse;
mod mpeg_dec;
mod uridecodebin3;
mod vorbis_dec;

use crate::{
    backend::mpeg_audio_parse::{init_gst, new_empty_stream},
    backend::mpeg_dec::new_empty_dec_stream,
};
use anyhow::Context;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

pub struct EncodingBackend {
    data_tx: mpsc::Sender<Vec<u8>>,
    url: String,
}

impl EncodingBackend {
    pub fn new(data_tx: mpsc::Sender<Vec<u8>>, url: String) -> Self {
        Self { data_tx, url }
    }

    pub async fn start(self, cancel: CancellationToken) {
        init_gst().expect("Failed to initialize GStreamer");
        loop {
            tokio::select! {
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
        tracing::debug!("Encoding pipeline created OK");
        let rx = stream_with_pipeline.sink_rx();
        tracing::debug!("Will start encoding main loop");
        tokio::spawn(async move {
            tracing::debug!("Now start encoding main loop");
            if let Err(err) = stream_with_pipeline.main_loop(cancel).await {
                tracing::error!("Error in main loop: {}", err);
            }
        });
        while let Ok(buf) = rx.recv_async().await {
            tracing::trace!("Received buffer of size {}", buf.len());
            self.data_tx.send(buf).await.map_err(|e| e.to_string())?;
        }

        Ok(())
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
