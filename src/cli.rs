use clap::{Args, Parser, Subcommand};
use libp2p::PeerId;
use std::fmt::Display;
use std::io;
use thiserror::Error;
use tokio::net;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(long = "http.addr", global = true, default_value = "0.0.0.0")]
    pub http_addr: String,

    #[arg(long = "http.port", global = true, default_value = "7890")]
    pub http_port: u16,

    // this is a limitation of clap: https://github.com/clap-rs/clap/issues/1546
    #[arg(short, long, global = true)]
    pub topic: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Listen to the p2p radio
    Listener(Listener),
    /// Go on air and stream radio
    Streamer(Streamer),
}

#[derive(Debug, Args)]
pub struct Listener {
    #[arg(short, long)]
    pub streamer_peer_id: PeerId,

    #[arg(long = "stream.addr", default_value = "0.0.0.0")]
    pub stream_addr: String,

    #[arg(long = "stream.port", default_value = "10909")]
    pub stream_port: u16,
}

#[derive(Debug, Args)]
pub struct Streamer {
    #[arg(long, default_value = "http://localhost:8000/main")]
    pub url: String,
}

impl Cli {
    pub fn http(&self) -> Http {
        Http {
            addr: self.http_addr.clone(),
            port: self.http_port,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Http {
    addr: String,
    port: u16,
}

#[derive(Debug, Error)]
pub enum HttpConversionError {
    #[error("lookup host error: {0}")]
    LookupHost(io::Error),
    #[error("other error: {0}")]
    Other(String),
}

impl Http {
    pub async fn into_socket_addr(self) -> Result<std::net::SocketAddr, HttpConversionError> {
        let host = format!("{}:{}", self.addr, self.port);
        let mut addrs = net::lookup_host(host)
            .await
            .map_err(HttpConversionError::LookupHost)?;
        let addr = addrs
            .next()
            .ok_or(HttpConversionError::Other("No address found".to_string()))?;
        Ok(addr)
    }
}

impl Display for Http {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.addr, self.port)
    }
}
