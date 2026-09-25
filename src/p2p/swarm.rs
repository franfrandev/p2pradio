use crate::AppError;
use crate::p2p::codec::Message;
use libp2p::futures::StreamExt;
use libp2p::gossipsub::{IdentTopic, MessageId};
use libp2p::kad::store::MemoryStore;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{PeerId, Swarm, SwarmBuilder, gossipsub, identify, kad, mdns, ping};
use thiserror::Error;
use tokio::io;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(NetworkBehaviour)]
pub struct RadioBehavior {
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    pub(crate) gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
}

#[derive(Debug, Error)]
pub enum GossipError {
    #[error(transparent)]
    PublishError(#[from] gossipsub::PublishError),
    #[error(transparent)]
    SubscriptionError(#[from] gossipsub::SubscriptionError),
    #[error("failed to subscribe to topic: already subscribed")]
    AlreadySubscribed,
}

pub(crate) fn create_swarm() -> Result<Swarm<RadioBehavior>, AppError> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| {
            let local_public_key = key.public();
            let local_id = local_public_key.to_peer_id();

            let identify = identify::Behaviour::new(identify::Config::new(
                "0.1.0".to_string(),
                local_public_key,
            ));

            let ping = ping::Behaviour::new(ping::Config::new());

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .message_id_fn(message_id)
                .validation_mode(gossipsub::ValidationMode::Strict)
                .build()
                .map_err(io::Error::other)?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_id)?;

            let kademlia = kad::Behaviour::new(local_id, MemoryStore::new(local_id));

            Ok(RadioBehavior {
                identify,
                ping,
                gossipsub,
                mdns,
                kademlia,
            })
        })
        .map_err(|err| AppError::Other(err.to_string()))?
        .build();

    let listener_id = swarm
        .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())
        .map_err(|err| AppError::Other(err.to_string()))?;
    tracing::debug!("Listening on {:?}", listener_id);

    Ok(swarm)
}

fn message_id(msg: &gossipsub::Message) -> MessageId {
    let Some(seq_no) = msg.sequence_number else {
        return MessageId::new(&[]);
    };
    MessageId::new(&seq_no.to_be_bytes())
}

pub async fn swarm_streamer(
    mut swarm: Swarm<RadioBehavior>,
    topic: IdentTopic,
    mut gossip_rx: mpsc::Receiver<Message>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            Some(msg) = gossip_rx.recv() => {
                let data = msg.encode();
                tracing::trace!("received data to publish: {} bytes", data.len());
                if let Err(err) = publish_data(&mut swarm, topic.clone(), data) {
                    tracing::debug!("Failed to publish data: {err}");
                }
            },
            event = swarm.select_next_some() => process_streamer_event(&mut swarm, event).await,
            _ = cancel.cancelled() => break,
        }
    }
}

pub async fn swarm_listener(
    mut swarm: Swarm<RadioBehavior>,
    streamer_peer_id: PeerId,
    gossip_tx: flume::Sender<Message>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            event = swarm.select_next_some() => process_listener_event(&mut swarm, streamer_peer_id, event, &gossip_tx).await,
            _ = cancel.cancelled() => break,
        }
    }
}

pub fn tune_in(swarm: &mut Swarm<RadioBehavior>, topic: &IdentTopic) -> Result<(), GossipError> {
    let subscribed = swarm.behaviour_mut().gossipsub.subscribe(topic)?;
    if !subscribed {
        return Err(GossipError::AlreadySubscribed);
    }
    Ok(())
}

fn publish_data(
    swarm: &mut Swarm<RadioBehavior>,
    topic: IdentTopic,
    data: Vec<u8>,
) -> Result<(), GossipError> {
    let id = swarm.behaviour_mut().gossipsub.publish(topic, data)?;
    tracing::debug!("Published data on with id {:?}", id);
    Ok(())
}

async fn process_streamer_event(
    swarm: &mut Swarm<RadioBehavior>,
    event: SwarmEvent<RadioBehaviorEvent>,
) {
    tracing::trace!("Swarm Event: {:?}", event);
    match event {
        SwarmEvent::Behaviour(b_event) => process_streamer_behavior_event(swarm, b_event).await,
        _other => {
            tracing::debug!("Swarm Event not handled: {:?}", _other)
        }
    }
}

async fn process_streamer_behavior_event(
    swarm: &mut Swarm<RadioBehavior>,
    b_event: RadioBehaviorEvent,
) {
    match b_event {
        RadioBehaviorEvent::Mdns(mdns_event) => process_mdns_event(swarm, mdns_event),
        _other => {
            tracing::debug!("Radio Behavior Event not handled: {:?}", _other)
        }
    }
}

async fn process_listener_event(
    swarm: &mut Swarm<RadioBehavior>,
    streamer_peer_id: PeerId,
    event: SwarmEvent<RadioBehaviorEvent>,
    gossip_tx: &flume::Sender<Message>,
) {
    tracing::trace!("Swarm Event: {:?}", event);
    match event {
        SwarmEvent::Behaviour(b_event) => {
            process_listener_behavior_event(swarm, streamer_peer_id, b_event, gossip_tx).await
        }
        _other => {
            tracing::debug!("Swarm Event not handled: {:?}", _other)
        }
    }
}

async fn process_listener_behavior_event(
    swarm: &mut Swarm<RadioBehavior>,
    streamer_peer_id: PeerId,
    b_event: RadioBehaviorEvent,
    gossip_tx: &flume::Sender<Message>,
) {
    match b_event {
        RadioBehaviorEvent::Mdns(mdns_event) => process_mdns_event(swarm, mdns_event),
        RadioBehaviorEvent::Gossipsub(gossip_sub_event) => {
            process_gossip_sub_event(swarm, gossip_sub_event, streamer_peer_id, gossip_tx).await
        }
        _other => {
            tracing::debug!("Radio Behavior Event not handled: {:?}", _other)
        }
    }
}

pub fn process_mdns_event(swarm: &mut Swarm<RadioBehavior>, mdns_event: mdns::Event) {
    match mdns_event {
        mdns::Event::Discovered(list) => {
            for (peer_id, _multiaddr) in list {
                tracing::debug!("mDNS discovered a new peer: {peer_id}");
                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            }
        }
        mdns::Event::Expired(list) => {
            for (peer_id, _multiaddr) in list {
                tracing::debug!("mDNS discover peer has expired: {peer_id}");
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .remove_explicit_peer(&peer_id);
            }
        }
    }
}

pub async fn process_gossip_sub_event(
    swarm: &mut Swarm<RadioBehavior>,
    gossip_sub_event: gossipsub::Event,
    streamer_peer_id: PeerId,
    gossip_tx: &flume::Sender<Message>,
) {
    match gossip_sub_event {
        gossipsub::Event::Message {
            propagation_source,
            message_id,
            message,
        } => {
            if propagation_source != streamer_peer_id {
                // TODO blacklist the peer if the source is not trusted
                tracing::error!("Got message from untrusted peer: {propagation_source}");
                let gossipsub = &mut swarm.behaviour_mut().gossipsub;
                gossipsub.blacklist_peer(&propagation_source);
                gossipsub.remove_explicit_peer(&propagation_source);
                return;
            }
            tracing::trace!("Got message with id: {message_id} from peer: {propagation_source}");
            let Some(seq_no) = message.sequence_number else {
                tracing::warn!("message seq no: None"); // TODO BLACKLIST
                return;
            };
            // TODO reorder messages based on seq_no while buffering according to max wait in case of drops
            tracing::debug!("message seq no: {seq_no}");
            let Ok(msg) = Message::decode(message.data) else {
                tracing::warn!("failed to decode message");
                return;
            };
            if let Err(err) = gossip_tx.send_async(msg).await {
                tracing::warn!("failed to send gossip message: {err}");
            }
        }
        _other => {
            tracing::debug!("GossipSub Event not handled: {:?}", _other)
        }
    }
}
