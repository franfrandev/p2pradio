use crate::AppError;
use libp2p::futures::StreamExt;
use libp2p::gossipsub::{IdentTopic, MessageId};
use libp2p::kad::store::MemoryStore;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{Swarm, SwarmBuilder, gossipsub, identify, kad, mdns, ping};
use rand::random;
use std::time::Duration;
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

    swarm
        .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())
        .map_err(|err| AppError::Other(err.to_string()))?;

    Ok(swarm)
}

fn message_id(msg: &gossipsub::Message) -> MessageId {
    let Some(seq_no) = msg.sequence_number else {
        return MessageId::new(&[]);
    };
    MessageId::new(&seq_no.to_be_bytes())
}

pub async fn swarm_loop(
    mut swarm: Swarm<RadioBehavior>,
    topic: IdentTopic,
    mut data_rx: mpsc::Receiver<Vec<u8>>,
    gossip_tx: Option<flume::Sender<Vec<u8>>>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            data = data_rx.recv() => {
                let Some(data) = data else { continue };
                if let Err(err) = publish_data(&mut swarm, topic.clone(), data) {
                    tracing::debug!("Failed to publish data: {err}");
                }
            },
            event = swarm.select_next_some() => process_event(&mut swarm, event, &gossip_tx).await,
        }
    }
}

pub fn tune_in(swarm: &mut Swarm<RadioBehavior>, topic: &IdentTopic) -> Result<(), GossipError> {
    swarm.behaviour_mut().gossipsub.subscribe(topic)?;
    Ok(())
}

fn publish_data(
    swarm: &mut Swarm<RadioBehavior>,
    topic: IdentTopic,
    data: Vec<u8>,
) -> Result<(), GossipError> {
    let _ = swarm.behaviour_mut().gossipsub.publish(topic, data)?;
    Ok(())
}

async fn process_event(
    swarm: &mut Swarm<RadioBehavior>,
    event: SwarmEvent<RadioBehaviorEvent>,
    gossip_tx: &Option<flume::Sender<Vec<u8>>>,
) {
    match event {
        SwarmEvent::Behaviour(b_event) => process_behavior_event(swarm, b_event, gossip_tx).await,
        _other => {
            // tracing::debug!("Swarm Event not handled: {:?}", _other)
        }
    }
}

async fn process_behavior_event(
    swarm: &mut Swarm<RadioBehavior>,
    b_event: RadioBehaviorEvent,
    gossip_tx: &Option<flume::Sender<Vec<u8>>>,
) {
    match b_event {
        RadioBehaviorEvent::Mdns(mdns_event) => process_mdns_event(swarm, mdns_event),
        RadioBehaviorEvent::Gossipsub(gossip_sub_event) => {
            process_gossip_sub_event(gossip_sub_event, gossip_tx).await
        }
        _other => {
            // tracing::debug!("Radio Behavior Event not handled: {:?}", _other)
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
    gossip_sub_event: gossipsub::Event,
    gossip_tx: &Option<flume::Sender<Vec<u8>>>,
) {
    match gossip_sub_event {
        gossipsub::Event::Message {
            propagation_source: peer_id,
            message_id: id,
            message,
        } => {
            // TODO blacklist the peer if the source is not trusted
            tracing::trace!("Got message with id: {id} from peer: {peer_id}",);
            let Some(seq_no) = message.sequence_number else {
                tracing::warn!("message seq no: None"); // TODO BLACKLIST
                return;
            };
            // TODO reorder messages based on seq_no while buffering according to max wait in case of drops
            tracing::debug!("message seq no: {seq_no}");
            if let Some(gossip_tx) = gossip_tx {
                gossip_tx.send_async(message.data).await.unwrap();
            }
        }
        _other => {
            // tracing::debug!("GossipSub Event not handled: {:?}", _other)
        }
    }
}
