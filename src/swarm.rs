use libp2p::futures::StreamExt;
use libp2p::{mdns, Swarm, gossipsub};
use libp2p::swarm::SwarmEvent;
use tokio_util::sync::CancellationToken;
use crate::RadioBehavior;

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
        _other => tracing::warn!("Swarm Event not handled: {:?}", _other),
    }
}

fn process_behavior_event(swarm: &mut Swarm<RadioBehavior>, b_event: RadioBehaviorEvent) {
    match b_event {
        RadioBehaviorEvent::Mdns(mdns_event) => process_mdns_event(swarm, mdns_event),
        RadioBehaviorEvent::GossipSub(gossip_sub_event) => process_gossip_sub_event(swarm, gossip_sub_event),
    }
}

pub fn process_mdns_event(swarm: &mut Swarm<RadioBehavior>, mdns_event: mdns::Event) {
    match mdns_event {
        mdns::Event::Discovered(list) => {
            for (peer_id, _multiaddr) in list {
                println!("mDNS discovered a new peer: {peer_id}");
                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            }
        },
        mdns::Event::Expired(list) => {
            for (peer_id, _multiaddr) in list {
                println!("mDNS discover peer has expired: {peer_id}");
                swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
            }
        },
    }
}

pub fn process_gossip_sub_event(gossip_sub_event: gossipsub::Event) {
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