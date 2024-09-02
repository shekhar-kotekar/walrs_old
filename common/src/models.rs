use std::time::SystemTime;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Message {
    pub payload: Bytes,
    pub key: Option<String>,
    pub timestamp: Option<u128>,
    offset: u64,
}

impl Message {
    pub fn new(payload: Bytes, key: Option<String>, timestamp: Option<u128>) -> Self {
        let message_timestamp = timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        });
        Message {
            payload,
            key,
            timestamp: Some(message_timestamp),
            offset: 0,
        }
    }
    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Batch {
    pub records: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum TopicCommand {
    CreateTopic { topic: Topic },
    WriteToTopic { topic_name: String },
    ReadFromTopic { topic_name: String },
}

impl From<Vec<u8>> for TopicCommand {
    fn from(bytes: Vec<u8>) -> Self {
        bincode::deserialize(&bytes).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Topic {
    pub name: String,
    pub num_partitions: Option<u8>,
    pub replication_factor: Option<u8>,
    pub retention_period: Option<u8>,
    pub batch_size: Option<u8>,
}

impl Topic {
    pub fn new(
        name: String,
        num_partitions: Option<u8>,
        replication_factor: Option<u8>,
        retention_period: Option<u8>,
        batch_size: Option<u8>,
    ) -> Self {
        let num_partitions = num_partitions.unwrap_or(3);
        let replication_factor = replication_factor.unwrap_or(2);
        let retention_period = retention_period.unwrap_or(24 * 7);
        let batch_size = batch_size.unwrap_or(10);
        Topic {
            name,
            num_partitions: Some(num_partitions),
            replication_factor: Some(replication_factor),
            retention_period: Some(retention_period),
            batch_size: Some(batch_size),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum BrokerResponse {
    TopicCreated { topic: Topic },
    TopicAlreadyExists { topic: Topic },
    TopicNotFound { topic_name: String },
    TopicDeleted { topic_name: String },
    TopicNotDeleted { topic_name: String },
    TopicList(Vec<Topic>),
    MessageBatchWriteSuccess,
    MessageBatchWriteFailure { error: String },
    SendMessageBatch,
}
