use clap::Parser;
use libp2p::gossipsub::IdentTopic;
use p2pradio::{AppError, Node, cli};
use std::error::Error;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = cli::Cli::parse();
    if cli.topic.is_none() {
        return Err(AppError::Other("missing --topic".to_string()).into());
    }
    tracing::debug!("cli: {:?}", cli);

    let ctrl_c = signal::ctrl_c();

    let cancel = CancellationToken::new();

    let topic = cli.topic.clone().expect("already checked");
    let topic = IdentTopic::new(topic);
    let node = Node::new(topic, cli.rpc, cli.http, cli.command)?;
    let node_handle = node.start(cancel.clone());

    let cancel2 = cancel.clone();
    tokio::spawn(async move {
        ctrl_c.await.expect("TODO: panic message");
        tracing::debug!("ctrl-c received");
        cancel2.cancel();
    });

    match node_handle.await {
        None => {} // cancelled
        Some(Ok(_)) => tracing::error!("program finished unexpectedly"),
        Some(Err(err)) => {
            tracing::error!("program panic, reason: {}", err);
        }
    }

    cancel.cancel(); // give tasks the termination signal in case we errored

    Ok(())
}
