use clap::Parser;
use p2pradio::cli;
use std::error::Error;
use tokio::{select, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = cli::Cli::parse();
    tracing::debug!("cli: {:?}", cli);

    let ctrl_c = signal::ctrl_c();

    let cancel = CancellationToken::new();

    let node = p2pradio::Node::run(cli, cancel.clone()).await?;

    select! {
        _ = ctrl_c => {
            tracing::debug!("ctrl-c received");
            cancel.cancel();
        }
        res = node.stopped() => {
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
