use std::fs;

use bytes::BytesMut;
use common::codecs::encoder::BatchEncoder;
use common::models::{Batch, Message};
use tokio::io::AsyncWriteExt;
use tokio::{fs::OpenOptions, sync::mpsc};
use tokio_util::codec::Encoder;
use tokio_util::sync::CancellationToken;

use crate::models::PartitionInfo;

pub async fn start_partition_writer(
    partition_info: PartitionInfo,
    mut peers_rx: mpsc::Receiver<Message>,
    cancellation_token: CancellationToken,
) {
    tracing::info!(
        "Starting partition manager for {} : {}",
        partition_info.topic.name,
        partition_info.partition_index
    );
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
    tracing::info!("Segment file path: {}", segment_file_path);
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(segment_file_path)
        .await
        .unwrap();
    let mut current_batch = Batch { records: vec![] };
    let mut batch_encoder = BatchEncoder {};
    loop {
        tokio::select! {
            Some(message) = peers_rx.recv() => {
                tracing::info!("Received message: {:?}", message);
                current_batch.records.push(message);
                if current_batch.records.len() >= partition_info.topic.batch_size.unwrap() as usize {
                    let mut encoded_batch = BytesMut::new();
                    match batch_encoder.encode(current_batch.clone(), &mut encoded_batch) {
                        Ok(_) => {
                            file.write_all(&encoded_batch)
                                .await
                                .expect("Failed to write to segment file");
                            file.flush().await.expect("Failed to flush segment file");
                            tracing::info!("Wrote batch of {} messages to file", current_batch.records.len());
                            current_batch = Batch { records: vec![] };
                        }
                        Err(e) => {
                            tracing::error!("Failed to encode batch: {:?}", e);
                        }
                    }
                } else {
                    tracing::info!("Batch size not reached yet. Current batch size: {}, batch size for topic: {}", current_batch.records.len(), partition_info.topic.batch_size.unwrap());
                }
            }
            _ = cancellation_token.cancelled() => {
                if !current_batch.records.is_empty() {
                    let mut encoded_batch = BytesMut::new();
                    match batch_encoder.encode(current_batch.clone(), &mut encoded_batch) {
                        Ok(_) => {
                            file.write_all(&encoded_batch)
                                .await
                                .expect("Failed to write to segment file");
                            file.flush().await.expect("Failed to flush segment file");
                            tracing::info!("Flushed last batch of {} messages to file", current_batch.records.len());
                        }
                        Err(e) => {
                            tracing::error!("Failed to encode batch: {:?}", e);
                        }
                    }
                }
                file.sync_all().await.expect("Failed to sync segment file");
                tracing::info!("file synced and shutdown");

                peers_rx.close();
                tracing::info!("peers_rx closed");
                tracing::info!("breaking out of partition manager for {}: {}", partition_info.topic.name, partition_info.partition_index);
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
    use common::models::{Message, Topic};
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

        let (peers_tx, peers_rx) = mpsc::channel::<Message>(3);
        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();

        let partition_manager_handle = tokio::spawn(async move {
            start_partition_writer(partition_info, peers_rx, cancellation_token_clone).await;
        });

        let message_1 = Message {
            payload: BytesMut::from("Message 1 without timestamp".as_bytes()).freeze(),
            key: None,
            timestamp: None,
        };
        let message_2 = Message {
            payload: BytesMut::from("Message 2 with timestamp".as_bytes()).freeze(),
            key: None,
            timestamp: Some(1234567890),
        };
        let mut encoded_batch = BytesMut::new();
        let mut batch_encoder = BatchEncoder {};
        let batch = Batch {
            records: vec![message_1.clone(), message_2.clone()],
        };
        batch_encoder.encode(batch, &mut encoded_batch).unwrap();

        peers_tx.send(message_1).await.unwrap();
        peers_tx.send(message_2).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        cancellation_token.cancel();
        match partition_manager_handle.await {
            Ok(_) => {
                let segment_file_path =
                    format!("{}/0/{}", log_dir_path.to_str().unwrap(), "segment_0.log");
                let file_contents = fs::read(segment_file_path).unwrap();
                assert_eq!(&file_contents, &encoded_batch);
            }
            Err(e) => {
                panic!("Partition manager failed with error: {:?}", e);
            }
        }
    }
}
