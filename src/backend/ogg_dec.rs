// Reconstruct an Ogg/Opus stream from individual RTP packets for HTTP listeners.

use crate::backend::OggHeaders;
use crate::backend::icecast_ogg::ErrorMessage;
use anyhow::Error;
use byte_slice_cast::AsSliceOf;
use futures_util::StreamExt;
use gstreamer as gst;
use gstreamer::bus::BusStream;
use gstreamer::element_error;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

pub struct EmptyDecStream {
    pub(crate) parsed_rx: mpsc::Receiver<Vec<u8>>,
    pub(crate) dec_tx: broadcast::Sender<Vec<u8>>,
}

pub struct DecStreamWithPipeline {
    pub(crate) parsed_rx: mpsc::Receiver<Vec<u8>>,
    pub(crate) pipeline: gst::Pipeline,
    pub(crate) app_src: gst_app::AppSrc,
}

pub struct DecStream<S> {
    pub(crate) state: S,
}

pub fn new_empty_dec_stream(
    parsed_rx: mpsc::Receiver<Vec<u8>>,
    dec_tx: broadcast::Sender<Vec<u8>>,
) -> DecStream<EmptyDecStream> {
    DecStream {
        state: EmptyDecStream { parsed_rx, dec_tx },
    }
}

impl DecStream<DecStreamWithPipeline> {
    #[instrument(level = "debug", skip_all)]
    pub async fn run_dec_stream(
        mut self: DecStream<DecStreamWithPipeline>,
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

        bus.add_signal_watch();
        bus.connect_message(None, |_, msg| {
            tracing::trace!("Received message: {:?}", msg);
        });

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

        Ok(())
    }

    async fn run_loop(
        &mut self,
        bus_stream: &mut BusStream,
        cancel: &CancellationToken,
    ) -> Result<bool, Error> {
        select! {
            Some(msg) = bus_stream.next() => process_message(msg),
            Some(bytes) = self.state.parsed_rx.recv() => process_bytes(&self.state.app_src, bytes),
            _ = cancel.cancelled() => {
                tracing::warn!("Cancelled in main_loop");
                Ok(true)
            }
        }
    }
}

pub fn process_message(msg: gstreamer::Message) -> Result<bool, Error> {
    use gst::MessageView;

    tracing::trace!("Received message: {:?}", msg);

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

impl DecStream<EmptyDecStream> {
    #[instrument(level = "debug", skip_all)]
    pub async fn create_opus_pipeline(
        self: DecStream<EmptyDecStream>,
        headers: OggHeaders,
    ) -> Result<DecStream<DecStreamWithPipeline>, Error> {
        tracing::debug!("Creating pipeline");

        let pipeline = gst::Pipeline::default();

        tracing::trace!("Creating app_src");
        let app_src = gst_app::AppSrc::builder()
            .name("app_src")
            .format(gst::Format::Time)
            .is_live(true)
            .do_timestamp(true)
            .caps(
                &gst::Caps::builder("application/x-rtp")
                    .field("media", "audio")
                    .field("clock-rate", 48000i32)
                    .field("payload", 96i32)
                    .field("encoding-name", "OPUS")
                    .build(),
            )
            .build();

        // Recover ordering and timestamps after transport drops GstBuffer metadata.
        let rtpjitterbuffer = gst::ElementFactory::make("rtpjitterbuffer")
            .name("rtpjitterbuffer")
            .build()?;

        let rtpopusdepay = gst::ElementFactory::make("rtpopusdepay")
            .name("rtpopusdepay")
            .build()?;
        // Keep the audio compressed; HTTP clients need Ogg pages, not PCM samples.
        let opusparse = gst::ElementFactory::make("opusparse").build()?;
        let oggmux = gst::ElementFactory::make("oggmux")
            .property("max-delay", 100_000_000u64)
            .property("max-page-delay", 100_000_000u64)
            .build()?;
        let outsink = gst_app::AppSink::builder()
            .name("output_sink")
            .sync(false)
            .build();

        let elements = &[
            app_src.upcast_ref(),
            &rtpjitterbuffer,
            &rtpopusdepay,
            &opusparse,
            &oggmux,
            outsink.upcast_ref(),
        ];
        pipeline.add_many(elements)?;
        gst::Element::link_many(elements)?;

        let dec_tx = self.state.dec_tx.clone();
        let mut last_was_header = false;
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
                            ("Failed to interpret buffer as bytes")
                        );

                        gst::FlowError::Error
                    })?;

                    tracing::trace!("samples: {:?}", samples);
                    tracing::debug!("samples: {}", samples.len());

                    // Share the lock with HTTP subscription so a joining listener
                    // receives each header exactly once, before any audio pages.
                    let mut cached_headers = headers.lock().expect("headers lock poisoned");
                    let is_header = buffer.flags().contains(gst::BufferFlags::HEADER);
                    if is_header {
                        if !last_was_header {
                            cached_headers.clear();
                        }
                        cached_headers.push(samples.to_vec());
                    }
                    last_was_header = is_header;

                    if let Err(err) = dec_tx.send(samples.to_vec()) {
                        tracing::debug!(
                            "Failed to send samples, expect loss. samples: {}, err: {}",
                            samples.len(),
                            err
                        );
                    }

                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        tracing::debug!("Pipeline linked");

        pipeline.set_state(gst::State::Ready)?;
        tracing::debug!("Pipeline created");

        Ok(DecStream::<DecStreamWithPipeline> {
            state: DecStreamWithPipeline {
                parsed_rx: self.state.parsed_rx,
                pipeline,
                app_src,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{OggHeaders, init_gst};
    use gstreamer as gst;
    use gstreamer_app as gst_app;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_opus_pipeline_outputs_ogg() -> Result<(), Error> {
        init_gst()?;

        // Same framing as the encoding side of the icecast pipeline.
        let enc = gst::parse::launch(
            "audiotestsrc num-buffers=100 ! audioconvert ! audioresample ! opusenc \
             ! rtpopuspay pt=96 ! appsink name=sink",
        )?
        .downcast::<gst::Pipeline>()
        .expect("not a pipeline");
        let sink = enc
            .by_name("sink")
            .expect("no sink")
            .downcast::<gst_app::AppSink>()
            .expect("not an appsink");
        let (gossip_tx, gossip_rx) = mpsc::channel(100);
        sink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |appsink| {
                    let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gst::FlowError::Error)?;
                    let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                    let _ = gossip_tx.send(map.to_vec());
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );

        let (dec_tx, mut dec_rx) = broadcast::channel(1000);
        let headers = OggHeaders::default();
        let dec = new_empty_dec_stream(gossip_rx, dec_tx.clone())
            .create_opus_pipeline(headers.clone())
            .await?;

        let cancel = CancellationToken::new();
        let cancel_ = cancel.clone();
        let decoder = tokio::spawn(async move { dec.run_dec_stream(cancel_).await });
        enc.set_state(gst::State::Playing)?;

        let first = timeout(Duration::from_secs(5), dec_rx.recv()).await??;
        let mut initial_stream = first.clone();
        // Wait until both headers and audio have been published.
        for _ in 0..3 {
            initial_stream.extend(timeout(Duration::from_secs(5), dec_rx.recv()).await??);
        }
        // Mirror HTTP: prepend cached headers, then receive live pages.
        let (mut late_stream, mut late_rx) = {
            let headers = headers.lock().unwrap();
            (headers.concat(), dec_tx.subscribe())
        };
        for _ in 0..3 {
            late_stream.extend(timeout(Duration::from_secs(5), late_rx.recv()).await??);
        }
        cancel.cancel();
        enc.set_state(gst::State::Null)?;
        timeout(Duration::from_secs(5), decoder).await???;

        assert!(first.starts_with(b"OggS"));
        let headers = headers.lock().unwrap();
        assert_eq!(headers.len(), 2);
        assert!(headers[0].windows(8).any(|w| w == b"OpusHead"));
        assert!(headers[1].windows(8).any(|w| w == b"OpusTags"));
        drop(headers);

        assert_ogg_decodes(initial_stream)?;
        assert_ogg_decodes(late_stream)?;

        Ok(())
    }

    fn assert_ogg_decodes(bytes: Vec<u8>) -> Result<(), Error> {
        let playback = gst::parse::launch(
            "appsrc name=source ! oggdemux ! opusdec ! appsink name=audio sync=false",
        )?
        .downcast::<gst::Pipeline>()
        .expect("not a pipeline");
        let source = playback
            .by_name("source")
            .unwrap()
            .downcast::<gst_app::AppSrc>()
            .unwrap();
        let audio = playback
            .by_name("audio")
            .unwrap()
            .downcast::<gst_app::AppSink>()
            .unwrap();
        playback.set_state(gst::State::Playing)?;
        source.push_buffer(gst::Buffer::from_mut_slice(bytes))?;
        source.end_of_stream()?;
        let sample = audio.try_pull_sample(gst::ClockTime::from_seconds(5));
        playback.set_state(gst::State::Null)?;
        let sample = sample.expect("Ogg stream should decode to audio");
        assert_eq!(
            sample.caps().unwrap().structure(0).unwrap().name(),
            "audio/x-raw"
        );
        assert!(sample.buffer().unwrap().size() > 0);
        Ok(())
    }
}
