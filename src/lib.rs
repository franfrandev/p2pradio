use crate::cli::HttpConversionError;
use libp2p::gossipsub::PublishError;
use thiserror::Error;
use tokio::io;

pub mod cli;
pub mod node;
use crate::p2p::swarm::GossipError;
pub use node::Node;

pub mod backend;
pub mod p2p;
pub mod rpc;

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    HttpConversionError(#[from] HttpConversionError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("app error: {0}")]
    Other(String),
    #[error("rpc error: {0}")]
    Rpc(#[from] rpc::RpcError),
    #[error(transparent)]
    PublishError(#[from] PublishError),
    #[error(transparent)]
    GossipError(#[from] GossipError),
}
