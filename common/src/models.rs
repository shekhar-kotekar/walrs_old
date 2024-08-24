use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Message {
    pub offset: u64,
    pub payload: Bytes,
    pub timestamp: u128,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Batch {
    pub base_offset: u64,
    pub last_offset: u64,
    pub count: u32,
    pub records: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Topic {
    pub name: String,
    pub num_partitions: u8,
    pub replication_factor: u8,
    pub retention_period: u16,
    pub batch_size: u16,
}

impl From<Vec<u8>> for Topic {
    fn from(bytes: Vec<u8>) -> Self {
        let topic_details: Topic = bincode::deserialize(&bytes).unwrap();
        if topic_details.name.is_empty() {
            panic!("Topic name cannot be empty");
        } else if topic_details.name.len() > 255 {
            panic!("Topic name cannot be more than 255 characters");
        } else if topic_details.num_partitions == 0 {
            panic!("Number of partitions cannot be 0");
        } else if topic_details.batch_size == 0 {
            panic!("Batch size cannot be 0");
        } else {
            topic_details
        }
    }
}
