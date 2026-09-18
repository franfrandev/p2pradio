// This module provides functionality for parsing audio data using the mpegaudioparse element from GStreamer.
// It provides seeking for MPEG files frames, as we cannot naively just cut frames in between because of the bit sink.
// The pipeline also has an app sink which allows then sampling these frames to write them into a buffer.
// https://gstreamer.freedesktop.org/documentation/audioparsers/mpegaudioparse.html?gi-language=c#mpegaudioparse-page

use anyhow::{Error};
use byte_slice_cast::AsSliceOf;
use derive_more::{Display, Error};
use gstreamer as gst;
use gstreamer::element_error;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing_gstreamer as tracing_gst;

pub struct EmptyStream {
    file: String,
}

pub struct StreamWithPipeline {
    pipeline: gst::Pipeline,
    sink_rx: flume::Receiver<Vec<u8>>,
}

pub struct Stream<S> {
    state: S,
}

#[derive(Debug, Display, Error)]
#[display("Main loop error from {src}: {error} (debug: {debug:?})")]
pub struct ErrorMessage {
    pub src: glib::GString,
    pub error: glib::Error,
    pub debug: Option<glib::GString>,
}

pub fn new_empty_stream(file: String) -> Stream<EmptyStream> {
    Stream {
        state: EmptyStream { file },
    }
}

pub fn init_gst() -> anyhow::Result<()> {
    // tracing_gst::integrate_spans();
    tracing_gst::integrate_events();
    // gst::log::remove_default_log_function();
    gst::init()?;
    Ok(())
}

impl Stream<EmptyStream> {
    #[instrument(level = "debug", skip_all)]
    pub fn create_pipeline(self: Stream<EmptyStream>) -> Result<Stream<StreamWithPipeline>, Error> {
        tracing::debug!("Creating encoding pipeline");

        let pipeline = gst::Pipeline::default();

        let filesrc = gst::ElementFactory::make("filesrc")
            .name("filesrc")
            .property("location", self.state.file)
            .build()?;
        let parser = gst::ElementFactory::make("mpegaudioparse")
            .name("parser")
            .build()?;
        let appsink = gst_app::AppSink::builder()
            .name("appsink")
            .caps(&gst::Caps::builder("audio/mpeg").build())
            .build();

        let elements = &[&filesrc, &parser, appsink.upcast_ref()];
        pipeline.add_many(elements)?;
        gst::Element::link_many(elements)?;

        let (sink_tx, sink_rx) = flume::bounded(100);

        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |appsink| {
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

                    if sink_tx.send(samples.to_vec()).is_err() {
                        tracing::error!(
                            "Failed to send samples, expect loss. samples: {}",
                            samples.len()
                        );
                    }

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        tracing::debug!("Encoding pipeline created");

        Ok(Stream::<StreamWithPipeline> {
            state: StreamWithPipeline { pipeline, sink_rx },
        })
    }
}

impl Stream<StreamWithPipeline> {
    #[instrument(level = "debug", skip_all)]
    pub async fn main_loop(
        self: Stream<StreamWithPipeline>,
        cancel: CancellationToken,
    ) -> Result<(), Error> {
        tracing::debug!("Starting main loop");

        tracing::debug!("Setting pipeline state to Playing");
        self.state.pipeline.set_state(gst::State::Playing)?;

        let bus = self
            .state
            .pipeline
            .bus()
            .expect("Pipeline without bus. Shouldn't happen!");

        let mut bus_stream = bus.stream();

        loop {
            select! {
                Some(msg) = bus_stream.next() => {
                    match process_message(&self.state.pipeline, msg) {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(err) => {
                            tracing::error!("Error processing message: {:?}", err);
                            break
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }

        tracing::debug!("Pipeline ended");

        self.state.pipeline.set_state(gst::State::Null)?;

        Ok(())
    }

    pub fn sink_rx(&self) -> flume::Receiver<Vec<u8>> {
        self.state.sink_rx.clone()
    }
}

fn process_message(pipeline: &gst::Pipeline, msg: gst::Message) -> Result<bool, anyhow::Error> {
    use gst::MessageView;

    tracing::debug!("Received message: {:?}", msg);

    match msg.view() {
        MessageView::Eos(..) => return Ok(true),
        MessageView::Error(err) => {
            pipeline.set_state(gst::State::Null)?;
            return Err(ErrorMessage {
                src: msg
                    .src()
                    .map(|s| s.path_string())
                    .unwrap_or_else(|| glib::GString::from("UNKNOWN")),
                error: err.error(),
                debug: err.debug(),
            }
            .into());
        }
        _ => (),
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;
    use tracing_subscriber::EnvFilter;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_main_loop() -> Result<(), Error> {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();

        init_gst()?;

        let root = env!("CARGO_MANIFEST_DIR");
        let empty_stream = new_empty_stream(format!("{}/tests/file_example_MP3_700KB.mp3", root));
        let stream_with_pipeline = empty_stream.create_pipeline()?;
        let rx = stream_with_pipeline.sink_rx();
        let cancel = CancellationToken::new();
        let cancel_ = cancel.clone();
        tokio::spawn(async move {
            if let Err(err) = stream_with_pipeline.main_loop(cancel_).await {
                tracing::error!("main_loop error: {:?}", err);
            }
        });

        let res = timeout(Duration::from_secs(2), rx.recv_async()).await;
        let Ok(_) = res else { panic!("timed out") };
        cancel.cancel();

        Ok(())
    }
}
