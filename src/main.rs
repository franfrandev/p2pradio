mod cli;
pub mod p2p;
pub mod rpc;

use crate::cli::{Commands, HttpConversionError};
use crate::p2p::swarm;
use crate::rpc::{ServerType, start_rpc_server};
use clap::Parser;
use libp2p::kad::store::MemoryStore;
use libp2p::{gossipsub, identify, kad, mdns, ping, swarm::NetworkBehaviour, tcp, yamux};
use p2p::swarm::swarm_loop;
use std::error::Error;
use thiserror::Error;
use tokio::io;
use tokio::{select, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(NetworkBehaviour)]
pub struct RadioBehavior {
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    HttpConversionError(#[from] HttpConversionError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("app error: {0}")]
    Other(String),
    #[error("gossipsub radio error: {0}")]
    GossipsubRadioError(#[from] p2p::gossip_sub::GossipSubRadioError),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = cli::Cli::parse();
    tracing::debug!("cli: {:?}", cli);

    let ctrl_c = signal::ctrl_c();

    let cancel = CancellationToken::new();

    select! {
        _ = ctrl_c => {
            tracing::debug!("ctrl-c received");
            cancel.cancel();
        }
        res = run_program(cli, cancel.clone()) => {
            match res {
                Ok(_) => {
                    tracing::error!("program finished");
                }
                Err(err) => {
                    tracing::error!("program finished, reason: {}", err);
                }
            };
        }
    }

    Ok(())
}

async fn run_program(cli: cli::Cli, cancel: CancellationToken) -> Result<(), AppError> {
    let swarm = {
        let swarm = swarm::create_swarm()?;
        let cancel = cancel.clone();
        tokio::spawn(async move {
            swarm_loop(swarm, cancel).await;
            tracing::debug!("swarm loop stopped");
        })
    };

    let rpc = {
        let server_type = match cli.command {
            Commands::Listener(..) => ServerType::Listener,
            Commands::Streamer(..) => ServerType::Streamer,
        };
        let addr = cli.http().into_socket_addr().await?;
        tracing::debug!("resolved socket addr: {}", addr);

        tokio::spawn(async move {
            let (addr, server_handle) = start_rpc_server(server_type, addr).await?;
            server_handle.stopped().await;
            tracing::debug!("rpc server stopped");
            Ok::<(), AppError>(())
        })
    };

    select! {
        res = swarm => if let Err(err) = res {
            tracing::error!("swarm task panic: {}", err);
        },
        tres = rpc => {
            match tres {
                Ok(res) => {
                    res?;
                    tracing::debug!("rpc task finished");
                }
                Err(err) => tracing::error!("rpc task panic: {}", err),
            }
        },
        _ = cancel.cancelled() => (),
    }

    Ok(())
}
