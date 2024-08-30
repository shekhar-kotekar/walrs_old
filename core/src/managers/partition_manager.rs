use std::fs;

use common::models::Message;
use tokio::io::AsyncWriteExt;
use tokio::{fs::OpenOptions, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::models::{ParentalCommands, PartitionInfo};

pub async fn start_partition_writer(
    partition_info: PartitionInfo,
    mut parent_rx: mpsc::Receiver<ParentalCommands>,
    mut peers_rx: mpsc::Receiver<Message>,
    cancellation_token: CancellationToken,
) {
    tracing::info!("Starting partition manager for : {:?}", partition_info,);
    match fs::create_dir_all(partition_info.partition_path.clone()) {
        Ok(_) => {
            tracing::info!(
                "Created partition directory: {}",
                partition_info.partition_path
            );
        }
        Err(e) => {
            println!(
                "Failed to create partition directory: {} with error: {:?}",
                partition_info.partition_path, e
            );
        }
    }
    let segment_file_path = format!("{}/{}", partition_info.partition_path, "segment_0.log");
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(segment_file_path)
        .await
        .unwrap();

    loop {
        tokio::select! {
            Some(command) = parent_rx.recv() => {
                tracing::info!("Received command: {:?}", command);
            }
            Some(message) = peers_rx.recv() => {
                tracing::debug!("Received message: {:?}", message);
                let message_bytes = bincode::serialize(&message).unwrap();
                file.write_all(&message_bytes).await.expect("Failed to write to segment file");
            }
            _ = cancellation_token.cancelled() => {
                tracing::info!("Cancellation token received for {:?} partition manager.", partition_info);
                file.flush().await.expect("Failed to flush segment file");
                parent_rx.close();
                peers_rx.close();
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use bytes::BytesMut;
    use common::models::{Batch, Message, Topic};
    use test_log::test;

    #[test(tokio::test)]
    async fn test_partition_manager_should_write_message_batch_to_file() {
        let topic_name = "test_topic".to_string();
        let temp_dir = tempdir::TempDir::new("log_dir_prefix").unwrap();
        let log_dir_path = temp_dir.path().join(topic_name.clone());
        fs::create_dir_all(log_dir_path.clone()).unwrap();

        let batch_size = 2;
        let test_topic = Topic::new(topic_name.clone(), None, None, None, Some(batch_size));

        let partition_info = PartitionInfo::new(
            test_topic,
            0,
            log_dir_path.as_path().to_str().unwrap().to_string(),
        );

        let (_, parent_rx) = mpsc::channel(5);
        let (peers_tx, peers_rx) = mpsc::channel::<Message>(5);
        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();

        let partition_manager_handle = tokio::spawn(async move {
            start_partition_writer(
                partition_info,
                parent_rx,
                peers_rx,
                cancellation_token_clone,
            )
            .await;
        });

        let message_1 = Message {
            payload: BytesMut::from("Message without timestamp".as_bytes()).freeze(),
            key: None,
            timestamp: None,
        };
        let message_2 = Message {
            payload: BytesMut::from("Message with timestamp".as_bytes()).freeze(),
            key: None,
            timestamp: Some(1234567890),
        };
        peers_tx.send(message_1).await.unwrap();
        peers_tx.send(message_2).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        cancellation_token.cancel();
        match partition_manager_handle.await {
            Ok(_) => {
                let segment_file_path =
                    format!("{}/0/{}", log_dir_path.to_str().unwrap(), "segment_0.log");
                let file_contents = fs::read(segment_file_path).unwrap();
                assert_eq!(file_contents, batch_bytes);
            }
            Err(e) => {
                panic!("Partition manager failed with error: {:?}", e);
            }
        }
    }
}
