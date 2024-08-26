use common::codecs::decoder::BatchDecoder;
use common::codecs::encoder::BatchEncoder;
use common::models::{Batch, Topic};
use futures::{SinkExt, StreamExt};
use tokio::fs::File;
use tokio::io::BufStream;
use tokio::{fs::OpenOptions, net::TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite};

use tokio_util::sync::CancellationToken;

use crate::models::{ParentalCommands, Partition};

#[derive(Debug)]
pub struct ProducerManager {
    parent_rx: tokio::sync::mpsc::Receiver<ParentalCommands>,
    cancellation_token: CancellationToken,
    log_directory_path: String,
}

impl ProducerManager {
    pub fn new(
        parent_rx: tokio::sync::mpsc::Receiver<ParentalCommands>,
        cancellation_token: CancellationToken,
        log_directory_path: String,
    ) -> Self {
        ProducerManager {
            parent_rx,
            cancellation_token,
            log_directory_path,
        }
    }

    fn get_log_file_path(&self, topic: Topic) -> String {
        format!("{}/{}/segment_0.log", self.log_directory_path, topic.name)
    }

    pub async fn serve(&mut self, stream: BufStream<TcpStream>, topic: Topic) {
        tracing::info!("ProducerManager serving topic: {:?}", topic);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.get_log_file_path(topic))
            .await
            .unwrap();

        let mut file_writer = FramedWrite::new(file, BatchEncoder {});
        let max_records_per_batch = 100;
        let mut current_batch = Batch {
            base_offset: 0,
            last_offset: 0,
            count: 0,
            records: vec![],
        };
        let mut message_reader = FramedRead::new(stream, BatchDecoder {});
        loop {
            tokio::select! {
                Some(batch) = message_reader.next() => {
                    let batch = batch.unwrap();
                    tracing::info!("Received batch: {:?}", batch);

                    current_batch.records.extend(batch.records);
                    current_batch.count += batch.count;
                    current_batch.last_offset = batch.last_offset;
                    let next_offset = current_batch.base_offset + 1;
                    if current_batch.count >= max_records_per_batch {
                        file_writer.send(current_batch).await.unwrap();
                        file_writer.flush().await.unwrap();

                        current_batch = Batch {
                            base_offset: next_offset,
                            last_offset: next_offset,
                            count: 0,
                            records: vec![],
                        };
                    }
                }
                Some(cmd) = self.parent_rx.recv() => {
                    match cmd {
                        ParentalCommands::Stop => {
                            self.cleanup(file_writer, current_batch).await;
                            break;
                        }
                        ParentalCommands::GetPartitionInfo {reply_tx} => {
                            let partition_info = Partition {
                                id: 0,
                                base_offset: current_batch.base_offset,
                                last_offset: current_batch.last_offset,
                                count: current_batch.count,
                            };
                            let _ = reply_tx.send(Some(partition_info));
                        }
                        _ => {
                            tracing::error!("ProducerManager cannot handle this command");
                        }
                    }
                }
                _ = self.cancellation_token.cancelled() => {
                    self.cleanup(file_writer, current_batch).await;
                    tracing::info!("ProducerManager shutting down due to cancellation");
                    break;
                }
            }
        }
    }
    async fn cleanup(
        &mut self,
        mut file_writer: FramedWrite<File, BatchEncoder>,
        last_batch: Batch,
    ) {
        tracing::info!("Cleaning up ProducerManager");
        file_writer.send(last_batch).await.unwrap();
        file_writer.flush().await.unwrap();
        self.parent_rx.close();
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::SystemTime};

    use common::models::{Batch, Message, Topic};
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
        sync::{mpsc, oneshot},
    };
    use tokio_util::sync::CancellationToken;

    use crate::models::{ParentalCommands, Partition};

    use super::ProducerManager;

    #[tokio::test]
    async fn test_producer_manager_should_return_current_batch_information() {
        let listener = TcpListener::bind("127.0.0.1:5056").await.unwrap();

        let stream = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();

        let (parent_tx, parent_rx) = mpsc::channel(10);
        let cancellation_token = CancellationToken::new();

        let topic_name: String = "dummy_topic".to_string();
        let temp_dir = tempdir::TempDir::new("").unwrap();

        let log_dir_path = temp_dir.path().join(topic_name.clone());
        fs::create_dir_all(log_dir_path.clone()).unwrap();

        let mut pm = ProducerManager::new(
            parent_rx,
            cancellation_token.clone(),
            temp_dir.path().to_str().unwrap().to_string(),
        );

        tokio::spawn(async move {
            let topic = Topic {
                name: topic_name.clone(),
                num_partitions: 1,
                replication_factor: 1,
                retention_period: 1,
                batch_size: 10,
            };
            let buf_stream = tokio::io::BufStream::new(stream);
            pm.serve(buf_stream, topic).await;
        });

        let (oneshot_tx, oneshot_rx) = oneshot::channel::<Option<Partition>>();

        let get_partition_info_command = ParentalCommands::GetPartitionInfo {
            reply_tx: oneshot_tx,
        };
        match parent_tx.send(get_partition_info_command).await {
            Ok(_) => {
                let partition_info = oneshot_rx.await.unwrap().unwrap();
                assert_eq!(partition_info.id, 0);
                assert_eq!(partition_info.base_offset, 0);
                assert_eq!(partition_info.last_offset, 0);
                assert_eq!(partition_info.count, 0);
                cancellation_token.cancel();
            }
            Err(_) => {
                cancellation_token.cancel();
                panic!("Failed to send GetPartitionInfo command");
            }
        }
    }
}
