use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use libp2p::PeerId;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

pub mod common;

#[tokio::test]
async fn streamer() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let streamer = common::new_streamer().await?;
    let client = HttpClient::builder().build(format!("http://{}", streamer.rpc_addr))?;
    let peer_id_string = client.request::<String, [(); 0]>("info_peerId", []).await?;
    let streamer_peer_id = PeerId::from_str(&peer_id_string)?;
    let mut listeners = Vec::new();
    for _ in 0..5 {
        let listener = common::new_listener(streamer_peer_id).await?;
        listeners.push(listener);
    }

    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("my_music.mp3");
    fs::copy("tests/Unknown Brother.mp3", file_path.clone()).await?;
    // let mut tmp_file = File::create(file_path.clone())?;
    // write!(tmp_file, "test")?;

    // TODO how to make sure that all of the peers have subscribed to start the test?
    sleep(Duration::from_secs(2)).await;

    // p2pradio::backend::hls::main()?;

    tokio::spawn(async move {
        client
            .request::<(), [String; 1]>(
                "streamer_broadcastFile",
                [file_path.into_os_string().into_string().unwrap()],
            )
            .await
    });

    // let rpc_uri = format!("http://{}", listeners[0].rpc_addr);
    // tracing::error!("rpc_uri: {}", rpc_uri);
    // let listener_client = HttpClient::builder().build(rpc_uri)?;
    // listener_client
    //     .request::<(), [(); 0]>("listener_listen", [])
    //     .await?;

    sleep(Duration::from_secs(500)).await;

    drop(tmp_dir);

    Ok(())
}
