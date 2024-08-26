use std::collections::HashMap;

use common::models::Topic;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

pub struct TopicsManager {
    topics: HashMap<String, Topic>,
}

impl TopicsManager {
    pub fn new() -> Self {
        TopicsManager {
            topics: HashMap::new(),
        }
    }

    pub async fn start_topic_manager(&mut self, mut parent_rx: Receiver<TopicManagerCommands>) {
        tracing::info!("Topic Manager started");
        loop {
            let command = parent_rx.recv().await.unwrap();
            match command {
                TopicManagerCommands::CreateTopic { topic, reply_tx } => {
                    let topic_name = topic.name.clone();
                    if self.topics.contains_key(topic_name.as_str()) {
                        reply_tx.send(None).unwrap();
                        tracing::warn!("{} Topic already exists", topic_name);
                        continue;
                    } else {
                        self.topics.insert(topic_name.clone(), topic.clone());
                        tracing::info!("{} Topic created", topic_name);
                        reply_tx.send(Some(topic)).unwrap();
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
                    if self.topics.contains_key(topic_name.as_str()) {
                        let topic = self.topics.get(&topic_name);
                        reply_tx.send(topic.cloned()).unwrap();
                    } else {
                        reply_tx.send(None).unwrap();
                    }
                }
            }
        }
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
    Stop,
}
