use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};

use common::models::{Message, Topic};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::managers::partition_manager::start_partition_writer;
use crate::models::PartitionInfo;

const PARTITION_MANAGER_CHANNEL_SIZE: usize = 1000;

pub struct TopicsManager {
    topics: HashMap<String, Topic>,
    cancellation_token: CancellationToken,
    log_dir_path: String,
    partition_client_tx: HashMap<String, Sender<Message>>,
    partition_manager_task_tracker: TaskTracker,
}

impl TopicsManager {
    pub fn new(log_dir_path: String, cancellation_token: CancellationToken) -> Self {
        TopicsManager {
            topics: HashMap::new(),
            cancellation_token,
            log_dir_path,
            partition_client_tx: HashMap::new(),
            partition_manager_task_tracker: TaskTracker::new(),
        }
    }

    pub async fn start_topics_manager(&mut self, mut parent_rx: Receiver<TopicManagerCommands>) {
        self.topics = self.read_all_topic_info();
        tracing::info!(
            "{} topics found in {}.",
            self.topics.len(),
            self.log_dir_path
        );
        tracing::info!("Topic Manager started");
        loop {
            tokio::select! {
                    Some(command) = parent_rx.recv() => {
                        match command {
                            TopicManagerCommands::CreateTopic { topic, reply_tx } => {
                                self.create_topic(topic, reply_tx).await;
                            }
                            TopicManagerCommands::GetAllTopics { reply_tx } => {
                                reply_tx.send(self.topics.clone()).unwrap();
                            }
                            TopicManagerCommands::GetPartitionManagerTx {
                                topic_name,
                                message_key,
                                reply_tx,
                            } => {
                                let partition_index = message_key
                                    .as_ref()
                                    .map(|key| {
                                        let mut hasher = DefaultHasher::new();
                                        hasher.write(key.as_bytes());
                                        let topic = self.topics.get(topic_name.as_str()).unwrap();
                                        (hasher.finish() % topic.num_partitions.unwrap() as u64) as u8
                                    })
                                    .unwrap_or(0);
                                let partition_name = format!("{}-{}", topic_name, partition_index);
                                if self.partition_client_tx.contains_key(&partition_name) {
                                    let client_tx = self.partition_client_tx.get(&partition_name).unwrap();
                                    reply_tx.send(Some(client_tx.to_owned())).unwrap();
                                } else {
                                    reply_tx.send(None).unwrap();
                                }
                            }
                            TopicManagerCommands::GetTopicInfo {
                                topic_name,
                                reply_tx,
                            } => {
                                if self.topics.contains_key(topic_name.as_str()) {
                                    let topic = self.topics.get(&topic_name);
                                    reply_tx.send(topic.cloned()).unwrap();
                                } else {
                                    reply_tx.send(None).unwrap();
                                }
                            }
                        }
                    }
                    _ = self.cancellation_token.cancelled() => {
                        tracing::info!("Cancellation token received for topic manager.");
                        self.partition_manager_task_tracker.close();
                        self.partition_manager_task_tracker.wait().await;
                        break;
                }
            }
        }
    }

    async fn create_topic(&mut self, topic: Topic, reply_tx: oneshot::Sender<Option<Topic>>) {
        let topic_name = topic.name.clone();

        if self.topics.contains_key(topic_name.as_str()) {
            tracing::warn!("{} Topic already exists", topic_name);
            let topic = self.topics.get(topic_name.as_str()).unwrap().to_owned();
            reply_tx.send(Some(topic)).unwrap();
        } else {
            for partition_index in 0..topic.num_partitions.unwrap() {
                let partition_name = format!("{}-{}", topic_name, partition_index);
                let (client_tx, client_rx) =
                    mpsc::channel::<Message>(PARTITION_MANAGER_CHANNEL_SIZE);
                self.partition_client_tx
                    .insert(partition_name.clone(), client_tx);
                let partition =
                    PartitionInfo::new(topic.clone(), partition_index, self.log_dir_path.clone());
                let cancellation_token_for_partition = self.cancellation_token.clone();
                self.partition_manager_task_tracker.spawn(async move {
                    start_partition_writer(partition, client_rx, cancellation_token_for_partition)
                        .await;
                });
            }
            self.serialize_topic_info_to_file(&topic);
            self.topics.insert(topic_name.clone(), topic.clone());
            tracing::info!("{} Topic created", topic_name);
            reply_tx.send(Some(topic)).unwrap();
        }
    }

    fn serialize_topic_info_to_file(&self, topic: &Topic) {
        let topic_file_path = format!("{}/{}.json", self.log_dir_path, topic.name);
        let serialized_topic = bincode::serialize(topic).unwrap();
        std::fs::write(topic_file_path, serialized_topic).unwrap();
    }

    fn deserialize_topic_from_file(&self, topic_name: &str) -> Option<Topic> {
        let topic_file_path = format!("{}/{}.json", self.log_dir_path, topic_name);
        if let Ok(serialized_topic) = std::fs::read(topic_file_path) {
            let topic: Topic = bincode::deserialize(&serialized_topic).unwrap();
            Some(topic)
        } else {
            None
        }
    }

    fn read_all_topic_info(&self) -> HashMap<String, Topic> {
        let mut topics = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(self.log_dir_path.clone()) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    let entry_name = entry.file_name().to_str().unwrap().to_owned();
                    if path.is_dir() && entry_name.starts_with("topic_") {
                        if let Some(topic) = self.deserialize_topic_from_file(&entry_name) {
                            tracing::info!("Found topic: {}", topic.name);
                            topics.insert(entry_name, topic);
                        }
                    }
                }
            }
        }
        topics
    }
}

pub enum TopicManagerCommands {
    CreateTopic {
        topic: Topic,
        reply_tx: oneshot::Sender<Option<Topic>>,
    },
    GetTopicInfo {
        topic_name: String,
        reply_tx: oneshot::Sender<Option<Topic>>,
    },
    GetAllTopics {
        reply_tx: oneshot::Sender<HashMap<String, Topic>>,
    },
    GetPartitionManagerTx {
        topic_name: String,
        message_key: Option<String>,
        reply_tx: oneshot::Sender<Option<Sender<Message>>>,
    },
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use bytes::BytesMut;
    use common::{codecs::decoder::BatchDecoder, models::Message};
    use test_log::test;
    use tokio::io::BufReader;
    use tokio_stream::StreamExt;
    use tokio_util::codec::{Decoder, LengthDelimitedCodec};

    #[test(tokio::test)]
    async fn test_topics_manager_should_return_partition_manager() {
        let temp_dir = tempdir::TempDir::new("log_dir_").unwrap();
        let log_dir_path = temp_dir.path().to_str().unwrap().to_string();
        let (parent_tx, parent_rx) = mpsc::channel(5);
        let cancellation_token = CancellationToken::new();

        let mut topics_manager =
            TopicsManager::new(log_dir_path.clone(), cancellation_token.clone());

        let topic_name = "test_topic".to_string();

        let topic = Topic {
            name: topic_name.clone(),
            num_partitions: Some(1),
            replication_factor: Some(1),
            retention_period: Some(1),
            batch_size: Some(2),
        };

        let topic_manager_handle = tokio::spawn(async move {
            topics_manager.start_topics_manager(parent_rx).await;
        });

        let (reply_tx, reply_rx) = oneshot::channel();
        parent_tx
            .send(TopicManagerCommands::CreateTopic {
                topic: topic.clone(),
                reply_tx: reply_tx,
            })
            .await
            .unwrap();

        let topic_received = reply_rx.await.unwrap().unwrap();
        assert_eq!(topic_received, topic);
        assert_eq!(topic.name, topic_name.clone());

        let (reply_tx, reply_rx) = oneshot::channel();
        let get_partition_writer_tx_command = TopicManagerCommands::GetPartitionManagerTx {
            topic_name: topic_name.clone(),
            message_key: None,
            reply_tx: reply_tx,
        };

        parent_tx
            .send(get_partition_writer_tx_command)
            .await
            .unwrap();
        let partition_writer_tx = reply_rx.await.unwrap().unwrap();

        let message_1 = Message::new(
            BytesMut::from("Message 1 without timestamp".as_bytes()).freeze(),
            Some("dummy_key".to_string()),
            None,
        );

        let message_2 = Message::new(
            BytesMut::from("Message 2 without key and with timestamp".as_bytes()).freeze(),
            None,
            Some(1234567890),
        );

        let message_3 = Message::new(
            BytesMut::from("Message 3 with timestamp".as_bytes()).freeze(),
            Some("dummy_key_2".to_string()),
            Some(1334567899),
        );
        partition_writer_tx.send(message_1.clone()).await.unwrap();
        partition_writer_tx.send(message_2.clone()).await.unwrap();
        partition_writer_tx.send(message_3.clone()).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        cancellation_token.cancel();

        topic_manager_handle.await.unwrap();

        let segment_file_path = format!(
            "{}/topic_{}/partition_0/segment_0.log",
            log_dir_path, topic_name
        );
        let file_contents = fs::read(segment_file_path).unwrap();
        let mut framed_reader = LengthDelimitedCodec::builder()
            .length_field_offset(0)
            .length_field_length(4)
            .length_adjustment(0)
            .new_read(BufReader::new(file_contents.as_slice()));

        let mut frame = framed_reader.next().await.unwrap().unwrap();

        let mut batch_decoder = BatchDecoder {};
        if let Some(decoded_batch) = batch_decoder.decode(&mut frame).unwrap() {
            assert_eq!(decoded_batch.records.len(), 2);
            assert_eq!(decoded_batch.records[0].payload, message_1.payload);
            assert_eq!(decoded_batch.records[1].payload, message_2.payload);
        }
        frame = framed_reader.next().await.unwrap().unwrap();
        if let Some(decoded_batch) = batch_decoder.decode(&mut frame).unwrap() {
            assert_eq!(decoded_batch.records.len(), 1);
            assert_eq!(decoded_batch.records[0].payload, message_3.payload);
        }
    }
}
