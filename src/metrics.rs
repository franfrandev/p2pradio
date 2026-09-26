use lazy_static::lazy_static;
use prometheus_client::registry::Registry;
use std::sync::Mutex;

lazy_static! {
    pub static ref REGISTRY: Mutex<Registry> = Mutex::new(Registry::default());
}

pub fn global_registry() -> &'static Mutex<Registry> {
    &REGISTRY
}
