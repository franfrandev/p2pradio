use clap::{Args, Parser, Subcommand};
use libp2p::PeerId;
use std::net::{SocketAddr, ToSocketAddrs};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Variant,

    /// HTTP socket of the JSON-RPC server
    #[arg(long, global = true, default_value = "0.0.0.0:7890", value_parser = socketaddr_value_parser)]
    pub rpc: SocketAddr,

    /// HTTP socket of the HTTP server
    #[arg(long, global = true, default_value = "0.0.0.0:10909", value_parser = socketaddr_value_parser)]
    pub http: SocketAddr,

    // this is a limitation of clap: https://github.com/clap-rs/clap/issues/1546
    #[arg(short, long, global = true)]
    pub topic: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum Variant {
    /// Listen to the p2p radio
    Listener(Listener),
    /// Go on air and stream radio
    Streamer(Streamer),
}

#[derive(Debug, Args)]
pub struct Listener {
    #[arg(short, long)]
    pub streamer_peer_id: PeerId,
}

#[derive(Debug, Args)]
pub struct Streamer {
    #[arg(long, default_value = "http://localhost:8000/main")]
    pub url: String,
}

pub(crate) fn socketaddr_value_parser(value: &str) -> Result<SocketAddr, eyre::Error> {
    match value.to_socket_addrs() {
        Ok(mut iter) => iter.next().ok_or(eyre::Error::msg(format!("\"{value}\""))),
        Err(e) => Err(eyre::Error::from(e).wrap_err(format!("\"{value}\""))),
    }
}
