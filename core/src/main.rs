use bytes::BytesMut;
use managers::producer_manager::ProducerManager;
use models::{ParentalCommands, TopicCommand};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod managers;
mod models;

#[tokio::main]
async fn main() {
    common::enable_tracing();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    let cancellation_token = CancellationToken::new();
    let log_dir_path = "./logs/".to_string();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let (_parent_tx, parent_rx) = mpsc::channel::<ParentalCommands>(100);
        let cancellation_token_clone = cancellation_token.clone();
        let cloned_log_dir_path = log_dir_path.clone();

        tokio::spawn(async move {
            let mut buf_stream = tokio::io::BufStream::new(stream);
            let mut message_buffer = BytesMut::with_capacity(1024);
            let num_bytes_read = buf_stream.read_buf(&mut message_buffer).await.unwrap();
            tracing::info!("Received {} bytes", num_bytes_read);
            let client_command = TopicCommand::from(message_buffer.to_vec());
            match client_command {
                TopicCommand::CreateTopic {
                    topic_name,
                    num_partitions,
                    batch_size,
                } => {
                    tracing::info!(
                        "Received a CreateTopic command: {} {} {}",
                        topic_name,
                        num_partitions,
                        batch_size
                    );
                }
                TopicCommand::WriteToTopic { topic_name } => {
                    tracing::info!("Received a WriteToTopic command: {}", topic_name);
                    let mut producer_manager = ProducerManager::new(
                        parent_rx,
                        cancellation_token_clone,
                        cloned_log_dir_path,
                    );
                    producer_manager.serve(buf_stream, topic_name).await;
                }
            }
        });
    }
}
