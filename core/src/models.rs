use common::models::Topic;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct PartitionInfo {
    pub topic: Topic,
    pub partition_index: u8,
    pub partition_path: String,
}

impl PartitionInfo {
    pub fn new(topic: Topic, partition_index: u8, log_dir_path: String) -> Self {
        let partition_path = format!("{}/{}", log_dir_path, partition_index);
        PartitionInfo {
            topic,
            partition_index,
            partition_path,
        }
    }
}

#[derive(Debug)]
pub enum ParentalCommands {
    GetTopicInfo {
        topic_name: String,
        reply_tx: oneshot::Sender<Option<Topic>>,
    },
    GetPartitionInfo {
        reply_tx: oneshot::Sender<PartitionInfo>,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum ClientType {
    Producer,
    Consumer,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum ClientRequests {
    InitiateWrite {
        topic: String,
    },
    Write {
        topic: String,
        message_batch: Vec<u8>,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum ClientResponses {
    TopicNotFound {
        topic: String,
    },
    WriteAccepted {
        topic: String,
    },
    WriteComplete {
        topic: String,
        num_messages_written: u32,
    },
}
