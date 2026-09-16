pub mod hls;

use std::path::Path;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub struct Backend {
    data_tx: mpsc::Sender<Vec<u8>>,
    file_rx: Option<mpsc::Receiver<(String, oneshot::Sender<Result<(), String>>)>>,
}

impl Backend {
    pub fn new(
        data_tx: mpsc::Sender<Vec<u8>>,
        file_rx: Option<mpsc::Receiver<(String, oneshot::Sender<Result<(), String>>)>>,
    ) -> Self {
        Self { data_tx, file_rx }
    }

    pub async fn start(mut self, cancel: CancellationToken) {
        loop {
            tokio::select! {
                Some((file_path, tx)) = Self::opt_file_rx_recv(&mut self.file_rx) => {
                    let result = self.process_file(file_path).await;
                    let _ = tx.send(result);
                }
                _ = cancel.cancelled() => break,
            }
        }
    }

    async fn opt_file_rx_recv(
        file_rx: &mut Option<mpsc::Receiver<(String, oneshot::Sender<Result<(), String>>)>>,
    ) -> Option<(String, oneshot::Sender<Result<(), String>>)> {
        match file_rx.as_mut() {
            Some(rx) => rx.recv().await,
            None => std::future::pending().await,
        }
    }

    async fn process_file(&self, file_path: String) -> Result<(), String> {
        tracing::info!("Processing file: {}", file_path);
        if !Path::new(&file_path).exists() {
            return Err("File does not exist".to_string());
        }
        let f = File::open(file_path).await.map_err(|e| e.to_string())?;
        let mut reader = BufReader::new(f);
        let mut buf = vec![0; 4196];
        while reader.read_buf(&mut buf).await.map_err(|e| e.to_string())? > 0 {
            self.data_tx.send(buf).await.map_err(|e| e.to_string())?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            buf = vec![0; 4196];
        }
        Ok(())
    }
}
