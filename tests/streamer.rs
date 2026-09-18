use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use libp2p::PeerId;
use p2pradio::backend::mpeg_audio_parse::init_gst;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

pub mod common;

#[tokio::test(flavor = "multi_thread")]
async fn streamer() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    init_gst()?;

    let streamer = common::new_streamer().await?;
    let client = HttpClient::builder().build(format!("http://{}", streamer.rpc_addr))?;
    let peer_id_string = client.request::<String, [(); 0]>("info_peerId", []).await?;
    let streamer_peer_id = PeerId::from_str(&peer_id_string)?;
    let mut listeners = Vec::new();
    for _ in 0..1 {
        let listener = common::new_listener(streamer_peer_id).await?;
        listeners.push(listener);
    }

    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("my_music.mp3");
    fs::copy(
        "/home/francois/RustroverProjects/p2pradio/tests/Unknown_Brother.mp3",
        file_path.clone(),
    )
    .await?;

    // TODO how to make sure that all of the peers have subscribed to start the test?
    sleep(Duration::from_secs(2)).await;

    let file_str = file_path.into_os_string().into_string().unwrap();
    tokio::spawn(async move {
        let file_str_ = file_str.clone();
        loop {
            client
                .request::<(), [String; 1]>("streamer_broadcastFile", [file_str_.clone()])
                .await
                .expect("Failed to broadcast file");

            sleep(Duration::from_secs(5)).await;
        }
    });

    let rpc_uri = format!("http://{}", listeners[0].rpc_addr);
    let listener_client = HttpClient::builder().build(rpc_uri)?;
    let local_addr = listener_client
        .request::<SocketAddr, [(); 0]>("listener_localAddr", [])
        .await?;
    tracing::info!("local_addr: http://{}", local_addr.to_string());

    sleep(Duration::from_secs(500)).await;

    drop(tmp_dir);

    Ok(())
}
