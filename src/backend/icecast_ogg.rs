// Parse the source Ogg/Opus stream and emit one RTP packet per transport message.

use anyhow::Error;
use byte_slice_cast::AsSliceOf;
use derive_more::{Display, Error};
use gstreamer as gst;
use gstreamer::element_error;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use std::sync::{Arc, Mutex};
use tokio::select;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

#[derive(Debug, Display, Error)]
#[display("Main loop error from {src}: {error} (debug: {debug:?})")]
pub struct ErrorMessage {
    pub src: glib::GString,
    pub error: glib::Error,
    pub debug: Option<glib::GString>,
}

#[derive(Clone, Debug, glib::Boxed)]
#[boxed_type(name = "ErrorValue")]
pub struct ErrorValue(pub Arc<Mutex<Option<Error>>>);

pub struct EmptyStream {}

pub struct StreamWithPipeline {
    pub pipeline: gst::Pipeline,
    pub sink_rx: flume::Receiver<Vec<u8>>,
    pub title_ch: (
        watch::Sender<Option<String>>,
        watch::Receiver<Option<String>>,
    ),
}

pub struct Stream<S> {
    pub state: S,
}

pub fn new_empty_stream() -> Stream<EmptyStream> {
    Stream {
        state: EmptyStream {},
    }
}

impl Stream<EmptyStream> {
    #[instrument(level = "debug", skip_all)]
    pub fn create_icecast_pipeline(
        self: Stream<EmptyStream>,
        url: String,
    ) -> Result<Stream<StreamWithPipeline>, Error> {
        tracing::debug!("Creating encoding pipeline");

        let pipeline = gst::Pipeline::default();

        let urisourcebin = gst::ElementFactory::make("urisourcebin")
            .name("urisourcebin")
            .property("uri", url)
            .build()?;
        let parsebin = gst::ElementFactory::make("parsebin")
            .name("parsebin")
            .build()?;
        let rtpopuspay = gst::ElementFactory::make("rtpopuspay")
            .name("rtpopuspay")
            .property("pt", 96u32)
            .build()?;
        let appsink = gst_app::AppSink::builder().name("appsink").build();

        pipeline.add_many([&urisourcebin, &parsebin, &rtpopuspay, appsink.upcast_ref()])?;

        gst::Element::link_many([&rtpopuspay, appsink.upcast_ref()])?;

        let parsebin_weak = parsebin.downgrade();
        urisourcebin.connect_pad_added(move |_, src_pad| {
            let Some(parsebin) = parsebin_weak.upgrade() else {
                return;
            };
            let sink_pad = parsebin
                .static_pad("sink")
                .expect("parsebin has no sink pad");

            if !sink_pad.is_linked() {
                if let Err(err) = src_pad.link(&sink_pad) {
                    tracing::error!("Failed to link urisourcebin to parsebin: {:?}", err);
                }
            }
        });

        let rtpopuspay_weak = rtpopuspay.downgrade();
        parsebin.connect_pad_added(move |bin, src_pad| {
            let Some(rtpopuspay) = rtpopuspay_weak.upgrade() else {
                return;
            };
            let sink_pad = rtpopuspay
                .static_pad("sink")
                .expect("rtpopuspay has no sink pad");

            // Unlink the old pad
            if sink_pad.is_linked() {
                if let Some(old_peer) = sink_pad.peer() {
                    if let Err(err) = old_peer.unlink(&sink_pad) {
                        tracing::error!("Failed to unlink old pad: {:?}", err);
                    }
                }
            }

            if let Err(err) = src_pad.link(&sink_pad) {
                element_error!(
                    bin,
                    gst::ResourceError::Failed,
                    ("Failed to link parsebin sink to rtpopuspay src: {}", err)
                );
            }
        });

        let (sink_tx, sink_rx) = flume::bounded(100);
        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |appsink| {
                    let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or_else(|| {
                        element_error!(
                            appsink,
                            gst::ResourceError::Failed,
                            ("Failed to get buffer")
                        );
                        gst::FlowError::Error
                    })?;

                    let map = buffer.map_readable().map_err(|_| {
                        element_error!(
                            appsink,
                            gst::ResourceError::Failed,
                            ("Failed to map buffer")
                        );
                        gst::FlowError::Error
                    })?;

                    let samples = map.as_slice_of::<u8>().map_err(|_| {
                        element_error!(
                            appsink,
                            gst::ResourceError::Failed,
                            ("Failed to cast buffer")
                        );
                        gst::FlowError::Error
                    })?;

                    if sink_tx.send(samples.to_vec()).is_err() {
                        tracing::debug!("Failed to send samples, channel closed.");
                    }

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        let (title_tx, title_rx) = watch::channel(None);

        tracing::debug!("Encoding pipeline created");

        Ok(Stream::<StreamWithPipeline> {
            state: StreamWithPipeline {
                pipeline,
                sink_rx,
                title_ch: (title_tx, title_rx),
            },
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
                    match self.process_message(msg) {
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

        // TODO need to ensure this is called when cancelled at any point
        self.state.pipeline.set_state(gst::State::Null)?;

        Ok(())
    }

    pub fn process_message(&self, msg: gstreamer::Message) -> Result<bool, Error> {
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
            MessageView::Tag(tag) => {
                if let Some(title) = tag.tags().get::<gst::tags::Title>() {
                    tracing::debug!("Received title tag: {:?}", title);
                    let title_tx = &self.state.title_ch.0;
                    title_tx
                        .send(Some(title.get().to_string()))
                        .expect("rx is held");
                }
            }
            _ => (),
        }

        Ok(false)
    }

    pub fn sink_rx(&self) -> flume::Receiver<Vec<u8>> {
        self.state.sink_rx.clone()
    }

    pub fn title_rx(&self) -> watch::Receiver<Option<String>> {
        self.state.title_ch.1.clone()
    }
}
