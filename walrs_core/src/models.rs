use common::models::Topic;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

pub enum ParentalCommands {
    Stop,
    GetTopicInfo {
        topic_name: String,
        reply_tx: oneshot::Sender<Option<Topic>>,
    },
    GetPartitionInfo {
        reply_tx: oneshot::Sender<Option<Partition>>,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum ClientType {
    Producer,
    Consumer,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum TopicCommand {
    CreateTopic {
        topic_name: String,
        num_partitions: u8,
        batch_size: u16,
    },
    WriteToTopic {
        topic_name: String,
    },
}

impl From<Vec<u8>> for TopicCommand {
    fn from(bytes: Vec<u8>) -> Self {
        bincode::deserialize(&bytes).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Partition {
    pub id: u8,
    pub base_offset: u64,
    pub last_offset: u64,
    pub count: u32,
}
