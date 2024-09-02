use std::collections::HashMap;
use std::fs;

use crate::models::PartitionInfo;
use bytes::BytesMut;
use common::codecs::decoder::BatchDecoder;
use common::codecs::encoder::BatchEncoder;
use common::models::{Batch, Message};
use futures::sink::SinkExt;
use tokio::io::{BufReader, BufWriter};
use tokio::{fs::OpenOptions, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::codec::Decoder;
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub async fn start_partition_writer(
    partition_info: PartitionInfo,
    mut peers_rx: mpsc::Receiver<Message>,
    cancellation_token: CancellationToken,
) {
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
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(segment_file_path)
        .await
        .unwrap();

    let mut framed_writer = LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_adjustment(0)
        .new_write(BufWriter::new(file));

    let mut current_batch = Batch { records: vec![] };
    let mut batch_encoder = BatchEncoder {};
    let mut message_offset = 0;
    loop {
        tokio::select! {
            Some(mut message) = peers_rx.recv() => {
                tracing::info!("Received message: {:?}", message);
                message.set_offset(message_offset);
                message_offset += 1;
                current_batch.records.push(message);
                if current_batch.records.len() >= partition_info.topic.batch_size.unwrap() as usize {
                    let mut encoded_batch = BytesMut::new();
                    match batch_encoder.encode(current_batch.clone(), &mut encoded_batch) {
                        Ok(_) => {
                            framed_writer.send(encoded_batch.freeze()).await.unwrap();
                            framed_writer.flush().await.unwrap();
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
                            framed_writer.send(encoded_batch.freeze()).await.unwrap();
                            tracing::info!("Flushed last batch of {} messages to file", current_batch.records.len());
                        }
                        Err(e) => {
                            tracing::error!("Failed to encode batch: {:?}", e);
                        }
                    }
                }
                framed_writer.flush().await.unwrap();
                tracing::info!("file synced and shutdown");

                peers_rx.close();
                tracing::info!("peers_rx closed");
                tracing::info!("breaking out of partition manager for {}: {}", partition_info.topic.name, partition_info.partition_index);
                break;
            }
        }
    }
}

pub enum PartitionManagerCommands {
    SendMessages {
        tx: mpsc::Sender<Batch>,
        client_id: Uuid,
    },
}

pub async fn start_partition_reader(
    partition_info: PartitionInfo,
    mut partition_manager_rx: mpsc::Receiver<PartitionManagerCommands>,
    cancellation_token: CancellationToken,
) {
    let segment_file_path = format!("{}/{}", partition_info.partition_path, "segment_0.log");
    tracing::info!(
        "partition reader - Segment file path: {}",
        segment_file_path
    );
    let file = OpenOptions::new()
        .read(true)
        .open(segment_file_path)
        .await
        .unwrap();

    let mut framed_reader = LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_adjustment(0)
        .new_read(BufReader::new(file));

    let mut batch_decoder = BatchDecoder {};

    tracing::info!(
        "Partition manager started for: {} topic, {} partition",
        partition_info.topic.name,
        partition_info.partition_index
    );
    let mut client_offsets: HashMap<Uuid, u64> = HashMap::new();
    loop {
        tokio::select! {
            Some(partition_manager_command) = partition_manager_rx.recv() => {
                match partition_manager_command {
                    PartitionManagerCommands::SendMessages { tx, client_id } => {
                        tracing::info!("Requst for messages received from consumer: {}", client_id);
                        let next_frame = framed_reader.next().await;
                        tracing::info!("Next frame: {:?}", next_frame);
                        let mut bytes_mut: BytesMut = next_frame.unwrap().unwrap();
                        if let Some(decoded_batch) = batch_decoder.decode(&mut bytes_mut).unwrap() {
                            let last_message_offset = decoded_batch.records.last().unwrap().get_offset();
                            tx.send(decoded_batch).await.unwrap();
                            client_offsets.insert(client_id, last_message_offset);
                        } else {
                            tracing::info!("No more messages to send to consumer: {}", client_id);
                        }

                    }
                }
            }
            _ = cancellation_token.cancelled() => {
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
    use common::{
        codecs::decoder::BatchDecoder,
        models::{Message, Topic},
    };
    use test_log::test;
    use tokio_util::codec::Decoder;
    use uuid::Uuid;

    #[test(tokio::test)]
    async fn test_partition_reader_should_be_able_to_send_messages_to_consumer() {
        let topic_name = "test_topic_for_read".to_string();
        let temp_dir = tempdir::TempDir::new("partition_reader_test").unwrap();
        fs::create_dir_all(temp_dir.path()).unwrap();

        let batch_size = 2;
        let test_topic = Topic::new(topic_name.clone(), Some(1), None, None, Some(batch_size));

        let partition_info =
            PartitionInfo::new(test_topic, 0, temp_dir.path().to_str().unwrap().to_string());
        let partition_info_for_reader = partition_info.clone();
        let (partition_writer_tx, partition_writer_rx) = mpsc::channel::<Message>(3);
        let cancellation_token = CancellationToken::new();
        let partition_writer_cancellation_token = cancellation_token.clone();

        let partition_writer_handle = tokio::spawn(async move {
            start_partition_writer(
                partition_info,
                partition_writer_rx,
                partition_writer_cancellation_token,
            )
            .await;
        });

        let message_1 = Message::new(
            BytesMut::from("partition reader - Message 1".as_bytes()).freeze(),
            None,
            None,
        );
        let message_2 = Message::new(
            BytesMut::from("partition reader - Message 2".as_bytes()).freeze(),
            None,
            Some(1234567890),
        );

        let message_3 = Message::new(
            BytesMut::from("partition reader - Message 3".as_bytes()).freeze(),
            None,
            Some(1294967890),
        );
        let message_4 = Message::new(
            BytesMut::from("partition reader - Message 4".as_bytes()).freeze(),
            None,
            Some(1294967890),
        );
        partition_writer_tx.send(message_1.clone()).await.unwrap();
        partition_writer_tx.send(message_2.clone()).await.unwrap();
        partition_writer_tx.send(message_3.clone()).await.unwrap();
        partition_writer_tx.send(message_4.clone()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let partition_reader_cancellation_token = cancellation_token.clone();
        let (partition_reader_tx, partition_reader_rx) =
            mpsc::channel::<PartitionManagerCommands>(3);
        let partition_reader_handle = tokio::spawn(async move {
            start_partition_reader(
                partition_info_for_reader,
                partition_reader_rx,
                partition_reader_cancellation_token,
            )
            .await;
        });
        let (batch_tx, mut batch_rx) = mpsc::channel::<Batch>(5);
        let command_for_partition_reader = PartitionManagerCommands::SendMessages {
            tx: batch_tx.clone(),
            client_id: Uuid::new_v4(),
        };
        partition_reader_tx
            .send(command_for_partition_reader)
            .await
            .unwrap();

        let batch = batch_rx.recv().await.unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].payload, message_1.payload);
        assert_eq!(batch.records[1].payload, message_2.payload);

        let command_for_partition_reader = PartitionManagerCommands::SendMessages {
            tx: batch_tx,
            client_id: Uuid::new_v4(),
        };

        partition_reader_tx
            .send(command_for_partition_reader)
            .await
            .unwrap();

        let batch_2 = batch_rx.recv().await.unwrap();
        assert_eq!(batch_2.records.len(), 2);
        assert_eq!(batch_2.records[0].payload, message_3.payload);
        assert_eq!(batch_2.records[1].payload, message_4.payload);

        cancellation_token.cancel();
        partition_writer_handle.await.unwrap();
        partition_reader_handle.await.unwrap();
    }

    #[test(tokio::test)]
    async fn test_partition_writer_should_write_message_batch_to_file() {
        let topic_name = "test_topic".to_string();
        let temp_dir = tempdir::TempDir::new("log_dir_prefix").unwrap();
        fs::create_dir_all(temp_dir.path()).unwrap();

        let batch_size = 2;
        let test_topic = Topic::new(topic_name.clone(), None, None, None, Some(batch_size));

        let partition_info =
            PartitionInfo::new(test_topic, 0, temp_dir.path().to_str().unwrap().to_string());

        let (peers_tx, peers_rx) = mpsc::channel::<Message>(3);
        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();

        let partition_writer_handle = tokio::spawn(async move {
            start_partition_writer(partition_info, peers_rx, cancellation_token_clone).await;
        });

        let message_1 = Message::new(BytesMut::from("Message 1".as_bytes()).freeze(), None, None);
        let message_2 = Message::new(
            BytesMut::from("Message 2".as_bytes()).freeze(),
            None,
            Some(1234567890),
        );
        peers_tx.send(message_1.clone()).await.unwrap();
        peers_tx.send(message_2.clone()).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        cancellation_token.cancel();
        match partition_writer_handle.await {
            Ok(_) => {
                let segment_file_path = format!(
                    "{}/topic_{}/partition_0/segment_0.log",
                    temp_dir.path().to_str().unwrap(),
                    topic_name
                );
                let file_contents = fs::read(segment_file_path).unwrap();
                let mut framed_reader = LengthDelimitedCodec::builder()
                    .length_field_offset(0)
                    .length_field_length(4)
                    .length_adjustment(0)
                    .new_read(BufReader::new(file_contents.as_slice()));
                let mut frame = framed_reader.next().await.unwrap().unwrap();
                let mut batch_decoder = BatchDecoder {};
                let mut decoded_batches = Vec::new();

                while let Some(decoded_batch) = batch_decoder.decode(&mut frame).unwrap() {
                    decoded_batches.push(decoded_batch);
                }
                assert_eq!(decoded_batches.len(), 1);
                assert_eq!(decoded_batches[0].records.len(), 2);
                assert_eq!(decoded_batches[0].records[0].payload, message_1.payload);
                assert_eq!(decoded_batches[0].records[1].payload, message_2.payload);
            }
            Err(e) => {
                panic!("Partition manager failed with error: {:?}", e);
            }
        }
    }
}
