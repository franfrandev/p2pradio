// This module provides functionality for parsing audio data using the mpegaudioparse element from GStreamer.
// It provides seeking for MPEG files frames, as we cannot naively just cut frames in between because of the bit sink.
// The pipeline also has an app sink which allows then sampling these frames to write them into a buffer.
// https://gstreamer.freedesktop.org/documentation/audioparsers/mpegaudioparse.html?gi-language=c#mpegaudioparse-page

use crate::backend::mpeg_audio_parse::ErrorMessage;
use anyhow::Error;
use byte_slice_cast::AsSliceOf;
use futures_util::StreamExt;
use gstreamer as gst;
use gstreamer::bus::BusStream;
use gstreamer::element_error;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gstreamer_audio as gst_audio;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

pub struct EmptyDecStream {
    mpeg_rx: flume::Receiver<Vec<u8>>,
    dec_tx: flume::Sender<Vec<u8>>,
}

pub struct DecStreamWithPipeline {
    mpeg_rx: flume::Receiver<Vec<u8>>,
    pipeline: gst::Pipeline,
    app_src: gst_app::AppSrc,
}

pub struct DecStream<S> {
    state: S,
}

pub fn new_empty_dec_stream(
    mpeg_rx: flume::Receiver<Vec<u8>>,
    dec_tx: flume::Sender<Vec<u8>>,
) -> DecStream<EmptyDecStream> {
    DecStream {
        state: EmptyDecStream { mpeg_rx, dec_tx },
    }
}

impl DecStream<EmptyDecStream> {
    #[instrument(level = "debug", skip_all)]
    pub async fn create_pipeline(
        self: DecStream<EmptyDecStream>,
    ) -> Result<DecStream<DecStreamWithPipeline>, Error> {
        tracing::debug!("Creating pipeline");

        let pipeline = gst::Pipeline::default();

        tracing::trace!("Creating app_src");
        let app_src = gst_app::AppSrc::builder().name("app_src").build();
        tracing::trace!("Creating parse");
        let parse = gst::ElementFactory::make("mpegaudioparse")
            .name("parse")
            .build()?;
        tracing::trace!("Creating dec");
        let outsink = gst_app::AppSink::builder().name("output_sink").build();

        let elements = &[app_src.upcast_ref(), &parse, outsink.upcast_ref()];
        pipeline.add_many(elements)?;
        gst::Element::link_many(elements)?;

        tracing::debug!("Pipeline linked");

        let dec_tx = self.state.dec_tx.clone();
        outsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |appsink| {
                    tracing::debug!("New sample");

                    let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or_else(|| {
                        element_error!(
                            appsink,
                            gst::ResourceError::Failed,
                            ("Failed to get buffer from appsink")
                        );

                        gst::FlowError::Error
                    })?;

                    let map = buffer.map_readable().map_err(|_| {
                        element_error!(
                            appsink,
                            gst::ResourceError::Failed,
                            ("Failed to map buffer readable")
                        );

                        gst::FlowError::Error
                    })?;

                    let samples = map.as_slice_of::<u8>().map_err(|_| {
                        element_error!(
                            appsink,
                            gst::ResourceError::Failed,
                            ("Failed to interpret buffer as S16 PCM")
                        );

                        gst::FlowError::Error
                    })?;

                    tracing::trace!("samples: {:?}", samples);
                    tracing::debug!("samples: {}", samples.len());

                    if dec_tx.send(samples.to_vec()).is_err() {
                        tracing::error!(
                            "Failed to send samples, expect loss. samples: {}",
                            samples.len()
                        );
                    }

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        app_src.set_state(gst::State::Ready)?;
        outsink.set_state(gst::State::Ready)?;

        tracing::debug!("Pipeline created");

        Ok(DecStream::<DecStreamWithPipeline> {
            state: DecStreamWithPipeline {
                mpeg_rx: self.state.mpeg_rx,
                pipeline,
                app_src,
            },
        })
    }
}

impl DecStream<DecStreamWithPipeline> {
    #[instrument(level = "debug", skip_all)]
    pub async fn run_dec_stream(
        self: DecStream<DecStreamWithPipeline>,
        cancel: CancellationToken,
    ) -> Result<(), Error> {
        tracing::debug!("Starting main loop");

        tracing::debug!("Setting pipeline state to Playing");
        self.state.pipeline.set_state(gst::State::Playing)?;
        self.state.app_src.set_state(gst::State::Playing)?;

        let bus = self
            .state
            .pipeline
            .bus()
            .expect("Pipeline without bus. Shouldn't happen!");

        let mut bus_stream = bus.stream();

        loop {
            let res = self.run_loop(&mut bus_stream, &cancel).await;
            match res {
                Ok(true) => break,
                Ok(false) => continue,
                Err(err) => {
                    tracing::error!("Error in run_loop: {:?}", err);
                    break;
                }
            }
        }

        tracing::debug!("Pipeline ended");

        self.state.pipeline.set_state(gst::State::Null)?;
        self.state.app_src.set_state(gst::State::Null)?;

        Ok(())
    }

    async fn run_loop(
        &self,
        bus_stream: &mut BusStream,
        cancel: &CancellationToken,
    ) -> Result<bool, Error> {
        select! {
            Some(msg) = bus_stream.next() => process_message(msg),
            Ok(bytes) = self.state.mpeg_rx.recv_async() => process_bytes(&self.state.app_src, bytes),
            _ = cancel.cancelled() => {
                tracing::warn!("Cancelled in main_loop");
                Ok(true)
            }
        }
    }
}

fn process_message(msg: gstreamer::Message) -> Result<bool, Error> {
    use gst::MessageView;

    tracing::debug!("Received message: {:?}", msg);

    match msg.view() {
        MessageView::Eos(..) => return Ok(true),
        MessageView::Error(err) => {
            tracing::error!("Received error message: {:?}", err);
            return Err::<_, Error>(
                ErrorMessage {
                    src: msg
                        .src()
                        .map(|s| s.path_string())
                        .unwrap_or_else(|| glib::GString::from("UNKNOWN")),
                    error: err.error(),
                    debug: err.debug(),
                }
                .into(),
            );
        }
        _ => (),
    }

    Ok(false)
}

fn process_bytes(app_src: &gst_app::AppSrc, bytes: Vec<u8>) -> Result<bool, Error> {
    tracing::debug!("Received bytes: {}", bytes.len());
    let mut buffer = gst::Buffer::with_size(bytes.len())?;
    buffer
        .get_mut()
        .ok_or(gst::FlowError::Eos)?
        .map_writable()
        .map_err(|_| gst::FlowError::Error)?
        .copy_from_slice(&bytes);
    let res = app_src.push_buffer(buffer);
    tracing::debug!("Pushed buffer: {:?}", res);
    res?;
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::backend::mpeg_audio_parse::init_gst;
    use tracing_subscriber::EnvFilter;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_main_loop() -> Result<(), Error> {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();

        init_gst()?;

        let empty_stream = crate::backend::mpeg_audio_parse::new_empty_stream(
            "/home/francois/RustroverProjects/p2pradio/tests/Unknown_Brother.mp3".to_string(),
        );
        let stream_with_pipeline = empty_stream.create_pipeline()?;
        let mpeg_rx = stream_with_pipeline.rx();
        let cancel = CancellationToken::new();
        let cancel_ = cancel.clone();
        tokio::spawn(async move {
            if let Err(err) = stream_with_pipeline.main_loop(cancel_).await {
                tracing::error!("Error in main from enc loop: {:?}", err);
            }
        });

        let (dec_tx, dec_rx) = flume::unbounded();
        let empty_dec_stream = new_empty_dec_stream(mpeg_rx, dec_tx);
        let dec_stream_with_pipeline = empty_dec_stream
            .create_pipeline()
            .await
            .expect("failed to create pipeline");

        let cancel_ = cancel.clone();
        tokio::spawn(async move {
            if let Err(err) = dec_stream_with_pipeline
                .run_dec_stream(cancel_.clone())
                .await
            {
                tracing::error!("Error in main from dec loop: {:?}", err);
            }
            cancel_.cancel();
        });

        tracing::debug!("WAITING FOR TIMEOUT");
        let res = timeout(Duration::from_secs(4), dec_rx.recv_async()).await;
        cancel.cancel();
        let Ok(Ok(recv)) = res else {
            tracing::error!("failed, reason: {:?}", res);
            return Err(anyhow!("failed, reason: {:?}", res));
        };
        tracing::debug!("received {:?}", recv.len());

        Ok(())
    }
}
