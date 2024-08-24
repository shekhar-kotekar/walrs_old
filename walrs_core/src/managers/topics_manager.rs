use std::collections::HashMap;

use common::models::Topic;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

struct TopicsManager {
    topics: HashMap<String, Topic>,
}

impl TopicsManager {
    pub fn new() -> Self {
        TopicsManager {
            topics: HashMap::new(),
        }
    }

    pub async fn start_topic_manager(&mut self, mut parent_rx: Receiver<TopicManagerCommands>) {
        loop {
            let command = parent_rx.recv().await.unwrap();
            match command {
                TopicManagerCommands::CreateTopic {
                    topic_name,
                    partition_count,
                    replication_factor,
                    retention_period,
                    batch_size,
                    reply_tx,
                } => {
                    if self.topics.contains_key(&topic_name) {
                        reply_tx.send(None).unwrap();
                        tracing::warn!("{} Topic already exists", topic_name);
                        continue;
                    } else {
                        let new_topic = Topic {
                            name: topic_name.clone(),
                            num_partitions: partition_count,
                            replication_factor,
                            retention_period,
                            batch_size,
                        };
                        self.topics.insert(topic_name, new_topic.clone());
                        reply_tx.send(Some(new_topic)).unwrap();
                    }
                }
                TopicManagerCommands::Stop => {
                    self.topics.clear();
                    tracing::info!("Topic Manager stopped");
                    break;
                }
                TopicManagerCommands::GetTopicInfo {
                    topic_name,
                    reply_tx,
                } => {
                    let topic = self.topics.get(&topic_name);
                    reply_tx.send(topic.cloned()).unwrap();
                }
                _ => {
                    tracing::error!("Topic manager cannot handle this command");
                }
            }
        }
    }
}

pub enum TopicManagerCommands {
    CreateTopic {
        topic_name: String,
        partition_count: u8,
        replication_factor: u8,
        retention_period: u16,
        batch_size: u16,
        reply_tx: oneshot::Sender<Option<Topic>>,
    },
    DeleteTopic {
        topic_name: String,
    },
    GetTopicInfo {
        topic_name: String,
        reply_tx: oneshot::Sender<Option<Topic>>,
    },
    Stop,
}
