use common::models::Topic;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct PartitionInfo {
    pub topic: Topic,
    pub partition_index: u8,
    pub partition_path: String,
}

impl PartitionInfo {
    pub fn new(topic: Topic, partition_index: u8, log_dir_path: String) -> Self {
        let partition_path = format!(
            "{}/topic_{}/partition_{}",
            log_dir_path, topic.name, partition_index
        );
        PartitionInfo {
            topic,
            partition_index,
            partition_path,
        }
    }
}
