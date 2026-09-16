use libp2p::PeerId;
use p2pradio::Node;
use p2pradio::cli::{Cli, Commands, Listener, Streamer};
use tokio_util::sync::CancellationToken;

pub async fn start_node(commands: Commands) -> Result<Node, anyhow::Error> {
    let cli = build_cli(commands, "test-topic".to_string())?;
    let node = Node::run(cli, CancellationToken::new()).await?;
    Ok(node)
}

pub async fn new_streamer() -> Result<Node, anyhow::Error> {
    let command = Commands::Streamer(Streamer {});
    start_node(command).await
}

pub async fn new_listener(streamer_peer_id: PeerId) -> Result<Node, anyhow::Error> {
    let command = Commands::Listener(Listener { streamer_peer_id });
    start_node(command).await
}

fn build_cli(command: Commands, topic: String) -> Result<Cli, anyhow::Error> {
    let cli = Cli {
        command,
        http_addr: "127.0.0.1".to_string(),
        http_port: 0,
        topic,
    };

    Ok(cli)
}
