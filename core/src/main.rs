use bytes::BytesMut;
use common::models::TopicCommand;
use managers::producer_manager::ProducerManager;
use managers::topics_manager::{TopicManagerCommands, TopicsManager};
use models::ParentalCommands;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

mod managers;
mod models;

#[tokio::main]
async fn main() {
    common::enable_tracing();

    let mut topics_manager = TopicsManager::new();
    let (topic_manager_tx, topic_manager_rx) = mpsc::channel::<TopicManagerCommands>(10);
    tokio::spawn(async move {
        topics_manager.start_topic_manager(topic_manager_rx).await;
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    let cancellation_token = CancellationToken::new();
    let log_dir_path = "./logs/".to_string();

    tracing::info!("Listening on: {}", listener.local_addr().unwrap());

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tracing::info!("Accepted a new connection");

        let (_parent_tx, parent_rx) = mpsc::channel::<ParentalCommands>(100);
        let cancellation_token_clone = cancellation_token.clone();
        let cloned_log_dir_path = log_dir_path.clone();
        let topic_manager_tx_clone = topic_manager_tx.clone();

        tokio::spawn(async move {
            let mut buf_stream = tokio::io::BufStream::new(stream);
            let mut message_buffer = BytesMut::with_capacity(56);
            let num_bytes_read = buf_stream.read_buf(&mut message_buffer).await.unwrap();
            tracing::info!("Received {} bytes", num_bytes_read);

            let client_command = TopicCommand::from(message_buffer.to_vec());
            match client_command {
                TopicCommand::CreateTopic { topic } => {
                    tracing::info!("Received a CreateTopic command: {:?}", topic);
                    let (reply_tx, reply_rx) = oneshot::channel();
                    topic_manager_tx_clone
                        .send(TopicManagerCommands::CreateTopic {
                            topic,
                            reply_tx: reply_tx,
                        })
                        .await
                        .unwrap();
                    let response = reply_rx.await.unwrap();
                    let response_bytes = bincode::serialize(&response).unwrap();

                    buf_stream.write_all(&response_bytes).await.unwrap();
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
