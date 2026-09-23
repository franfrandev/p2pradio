// This module provides functionality for parsing audio data using the mpegaudioparse element from GStreamer.
// It provides seeking for MPEG files frames, as we cannot naively just cut frames in between because of the bit sink.
// The pipeline also has an app sink which allows then sampling these frames to write them into a buffer.
// https://gstreamer.freedesktop.org/documentation/audioparsers/mpegaudioparse.html?gi-language=c#mpegaudioparse-page

use crate::backend::mpeg_audio_parse::{EmptyStream, ErrorMessage, Stream, StreamWithPipeline};
use anyhow::Error;
use byte_slice_cast::AsSliceOf;
use derive_more::{Display, Error};
use gstreamer as gst;
use gstreamer::element_error;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use std::sync::{Arc, Mutex};
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing_gstreamer as tracing_gst;

pub fn new_empty_stream() -> Stream<EmptyStream> {
    Stream {
        state: EmptyStream {},
    }
}

pub fn init_gst() -> anyhow::Result<()> {
    // tracing_gst::integrate_spans();
    tracing_gst::integrate_events();
    // gst::log::remove_default_log_function();
    gst::init()?;
    Ok(())
}

#[derive(Clone, Debug, glib::Boxed)]
#[boxed_type(name = "ErrorValue")]
pub struct ErrorValue(pub Arc<Mutex<Option<Error>>>);

impl Stream<EmptyStream> {
    #[instrument(level = "debug", skip_all)]
    pub fn create_icecast_pipeline(
        self: Stream<EmptyStream>,
        url: String,
    ) -> Result<Stream<StreamWithPipeline>, Error> {
        tracing::debug!("Creating encoding pipeline");

        let pipeline = gst::Pipeline::default();

        let souphttpsrc = gst::ElementFactory::make("souphttpsrc")
            .name("souphttpsrc")
            .property("location", url)
            .property("iradio-mode", true)
            .build()?;
        let decodebin = gst::ElementFactory::make("decodebin")
            .name("decodebin")
            .build()?;

        let elements = &[&souphttpsrc, &decodebin];
        pipeline.add_many(elements)?;
        gst::Element::link_many(elements)?;

        let (sink_tx, sink_rx) = flume::bounded(100);

        let pipeline_weak = pipeline.downgrade();
        let sink_tx2 = sink_tx.clone();
        decodebin.connect_pad_added(move |dbin, src_pad| {
            let Some(pipeline) = pipeline_weak.upgrade() else {
                return;
            };

            let add_sink = |sink_tx3: flume::Sender<Vec<u8>>| -> Result<(), Error> {
                let audioconvert = gst::ElementFactory::make("audioconvert")
                    .name("audioconvert")
                    .build()?;
                let audioresample = gst::ElementFactory::make("audioresample")
                    .name("audioresample")
                    .build()?;
                let opusenc = gst::ElementFactory::make("opusenc")
                    .name("opusenc")
                    .build()?;
                let rtpopuspay = gst::ElementFactory::make("rtpopuspay")
                    .name("rtpopuspay")
                    .build()?;
                let rptstreampay = gst::ElementFactory::make("rtpstreampay")
                    .name("rtpstreampay")
                    .build()?;
                let appsink = gst_app::AppSink::builder().name("appsink").build();

                let elements = &[
                    &audioconvert,
                    &audioresample,
                    &opusenc,
                    &rtpopuspay,
                    &rptstreampay,
                    appsink.upcast_ref(),
                ];
                pipeline.add_many(elements)?;
                gst::Element::link_many(elements)?;

                for e in elements {
                    e.sync_state_with_parent()?;
                }

                let sink_pad = audioconvert
                    .static_pad("sink")
                    .expect("queue has no sinkpad");
                src_pad.link(&sink_pad)?;

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

                            if sink_tx3.send(samples.to_vec()).is_err() {
                                tracing::error!(
                                    "Failed to send samples, expect loss. samples: {}",
                                    samples.len()
                                );
                            }

                            Ok(gst::FlowSuccess::Ok)
                        })
                        .build(),
                );

                Ok(())
            };

            if let Err(err) = add_sink(sink_tx2.clone()) {
                element_error!(
                    dbin,
                    gst::ResourceError::Failed,
                    ("Failed to add sink elements"),
                    details: gst::Structure::builder("error-details")
                        .field("error",
                               ErrorValue(Arc::new(Mutex::new(Some(err)))))
                        .build()
                );
            }
        });

        tracing::debug!("Encoding pipeline created");

        Ok(Stream::<StreamWithPipeline> {
            state: StreamWithPipeline { pipeline, sink_rx },
        })
    }
}
