use std::fs;

use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub enum ConsumerManagerCommand {
    ReadMessages { topic_name: String },
}

pub async fn start_consumer_manager(
    log_dir_path: String,
    mut consumer_manager_rx: mpsc::Receiver<ConsumerManagerCommand>,
    cancellation_token: CancellationToken,
) {
    let partition_reader_task_tracker: TaskTracker = TaskTracker::new();
    let partition_readers = get_partition_paths(log_dir_path);

    loop {
        tokio::select! {
            Some(consumer_manager_command) = consumer_manager_rx.recv() => {
                match consumer_manager_command {
                    ConsumerManagerCommand::ReadMessages { topic_name } => {
                        tracing::info!("Sending message to topic: {}", topic_name);
                    }
                }
            }
            _ = cancellation_token.cancelled() => {
                tracing::info!("breaking out of consumer manager");
                partition_reader_task_tracker.close();
                partition_reader_task_tracker.wait().await;
                break;
            }
        }
    }
}

fn get_partition_paths(log_dir_path: String) -> Vec<String> {
    fn recursive_search(dir_path: &str, paths: &mut Vec<String>) {
        if let Ok(entries) = fs::read_dir(dir_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_dir() {
                        let path_str = path.to_str().unwrap().to_string();
                        if entry
                            .file_name()
                            .to_str()
                            .unwrap()
                            .starts_with("partition_")
                        {
                            paths.push(path_str);
                        } else {
                            recursive_search(&path_str, paths);
                        }
                    }
                }
            }
        }
    }
    let mut partition_paths = Vec::new();
    recursive_search(&log_dir_path, &mut partition_paths);
    partition_paths
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_partition_paths() {
        let topic_name = "test_topic".to_string();
        let temp_dir = tempdir::TempDir::new("log_dir_prefix").unwrap();
        let log_dir_path = temp_dir.path().join(topic_name.clone());
        let partition_0_path = log_dir_path.join("partition_0");
        let partition_1_path = log_dir_path.join("partition_1");
        fs::create_dir_all(partition_0_path.clone()).unwrap();
        fs::create_dir_all(partition_1_path.clone()).unwrap();

        let partition_paths = get_partition_paths(temp_dir.path().to_str().unwrap().to_string());
        assert_eq!(partition_paths.len(), 2);
        assert!(partition_paths.contains(&partition_0_path.to_str().unwrap().to_string()));
        assert!(partition_paths.contains(&partition_1_path.to_str().unwrap().to_string()));
    }
}
