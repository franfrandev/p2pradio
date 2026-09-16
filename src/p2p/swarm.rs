use crate::{AppError, RadioBehavior, RadioBehaviorEvent};
use libp2p::futures::StreamExt;
use libp2p::kad::store::MemoryStore;
use libp2p::swarm::SwarmEvent;
use libp2p::{Swarm, SwarmBuilder, gossipsub, identify, kad, mdns, noise, ping, tcp, yamux};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;
use tokio::io;
use tokio_util::sync::CancellationToken;

pub(crate) fn create_swarm() -> Result<Swarm<RadioBehavior>, AppError> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|err| AppError::Other(err.to_string()))? // TODO better typing
        .with_quic()
        .with_behaviour(|key| {
            let local_public_key = key.public();
            let local_id = local_public_key.to_peer_id();

            let identify = identify::Behaviour::new(identify::Config::new(
                "0.1.0".to_string(),
                local_public_key,
            ));

            let ping = ping::Behaviour::new(ping::Config::new());

            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10))
                .validation_mode(gossipsub::ValidationMode::Strict) // TODO we only need source validation
                .message_id_fn(message_id_fn)
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
        .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
        .map_err(|err| AppError::Other(err.to_string()))?;
    swarm
        .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())
        .map_err(|err| AppError::Other(err.to_string()))?;

    Ok(swarm)
}

pub async fn swarm_loop(mut swarm: Swarm<RadioBehavior>, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            event = swarm.select_next_some() => process_event(&mut swarm, event),
        }
    }
}

fn process_event(swarm: &mut Swarm<RadioBehavior>, event: SwarmEvent<RadioBehaviorEvent>) {
    match event {
        SwarmEvent::Behaviour(b_event) => process_behavior_event(swarm, b_event),
        SwarmEvent::NewListenAddr {
            listener_id,
            address,
        } => {
            tracing::debug!("Listening on {address} with id: {listener_id}");
        }
        _other => tracing::warn!("Swarm Event not handled: {:?}", _other),
    }
}

fn process_behavior_event(swarm: &mut Swarm<RadioBehavior>, b_event: RadioBehaviorEvent) {
    match b_event {
        RadioBehaviorEvent::Mdns(mdns_event) => process_mdns_event(swarm, mdns_event),
        RadioBehaviorEvent::Gossipsub(gossip_sub_event) => {
            process_gossip_sub_event(swarm, gossip_sub_event)
        }
        _other => tracing::warn!("Radio Behavior Event not handled: {:?}", _other),
    }
}

pub fn process_mdns_event(swarm: &mut Swarm<RadioBehavior>, mdns_event: mdns::Event) {
    match mdns_event {
        mdns::Event::Discovered(list) => {
            for (peer_id, _multiaddr) in list {
                println!("mDNS discovered a new peer: {peer_id}");
                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            }
        }
        mdns::Event::Expired(list) => {
            for (peer_id, _multiaddr) in list {
                println!("mDNS discover peer has expired: {peer_id}");
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .remove_explicit_peer(&peer_id);
            }
        }
    }
}

pub fn process_gossip_sub_event(
    swarm: &mut Swarm<RadioBehavior>,
    gossip_sub_event: gossipsub::Event,
) {
    match gossip_sub_event {
        gossipsub::Event::Message {
            propagation_source: peer_id,
            message_id: id,
            message,
        } => println!(
            "Got message: '{}' with id: {id} from peer: {peer_id}",
            String::from_utf8_lossy(&message.data),
        ),
        _other => tracing::warn!("GossipSub Event not handled: {:?}", _other),
    }
}
