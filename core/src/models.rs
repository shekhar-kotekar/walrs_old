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
pub struct Partition {
    pub id: u8,
    pub base_offset: u64,
    pub last_offset: u64,
    pub count: u32,
}
