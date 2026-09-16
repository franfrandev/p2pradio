use crate::{AppError, RadioBehavior};
use libp2p::gossipsub::SubscriptionError;
use libp2p::{Swarm, gossipsub};
use thiserror::Error;

pub struct GossipSubRadio {
    //
}

#[derive(Debug, Error)]
pub enum GossipSubRadioError {
    #[error(transparent)]
    Subscription(#[from] SubscriptionError)
}

impl GossipSubRadio {
    pub fn new() -> GossipSubRadio {
        todo!()
    }

    pub fn tune_in(&mut self, swarm: &mut Swarm<RadioBehavior>, topic_str: String) -> Result<(), GossipSubRadioError> {
        let topic = gossipsub::IdentTopic::new(topic_str);
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
        Ok(())
    }

    pub fn run(&self) {
        loop {}
    }
}
