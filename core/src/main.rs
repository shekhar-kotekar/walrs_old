use bytes::BytesMut;
use common::models::{Message, Topic, TopicCommand};
use managers::topics_manager::{TopicManagerCommands, TopicsManager};
use models::{ClientResponses, ParentalCommands};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

mod managers;
mod models;

#[tokio::main]
async fn main() {
    common::enable_tracing();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut signal = signal(SignalKind::terminate()).unwrap();
        signal.recv().await;
        tracing::info!("Received SIGTERM, shutting down");
        shutdown_tx.send(()).unwrap();
    });
    let cancellation_token = CancellationToken::new();
    let cancellation_token_for_shutdown = cancellation_token.clone();

    tokio::spawn(async move {
        shutdown_rx.await.unwrap();
        tracing::info!("Shutting down gracefully");
        cancellation_token_for_shutdown.cancel();
    });
    let log_dir_path = "./logs/".to_string();
    let mut topics_manager = TopicsManager::new(log_dir_path, cancellation_token.clone());
    let (topic_manager_tx, topic_manager_rx) = mpsc::channel::<TopicManagerCommands>(10);
    tokio::spawn(async move {
        topics_manager.start_topics_manager(topic_manager_rx).await;
    });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    let log_dir_path = "./logs/".to_string();

    tracing::info!("Listening on: {}", listener.local_addr().unwrap());

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        handle_client_connection(
            socket,
            cancellation_token.clone(),
            log_dir_path.clone(),
            topic_manager_tx.clone(),
        )
        .await;
    }
}

async fn handle_client_connection(
    socket: TcpStream,
    cancellation_token: CancellationToken,
    log_dir_path: String,
    topic_manager_tx: mpsc::Sender<TopicManagerCommands>,
) {
    tracing::info!("Accepted a new connection");
    let (_, parent_rx) = mpsc::channel::<ParentalCommands>(100);

    tokio::spawn(async move {
        let mut buf_stream = tokio::io::BufStream::new(socket);
        let mut message_buffer = BytesMut::with_capacity(56);
        let num_bytes_read = buf_stream.read_buf(&mut message_buffer).await.unwrap();
        tracing::info!("Received {} bytes", num_bytes_read);

        let client_command = TopicCommand::from(message_buffer.to_vec());

        match client_command {
            TopicCommand::CreateTopic { topic } => {
                handle_create_topic_request(topic, topic_manager_tx, buf_stream).await;
            }
            TopicCommand::WriteToTopic {
                topic_name,
                messages,
            } => {
                handle_write_to_topic_request(
                    topic_name,
                    messages,
                    topic_manager_tx,
                    buf_stream.into_inner(),
                    parent_rx,
                    cancellation_token,
                    log_dir_path,
                )
                .await;
            }
        }
    });
}

async fn handle_write_to_topic_request(
    topic_name: String,
    messages: Vec<Message>,
    topic_manager_tx_clone: mpsc::Sender<TopicManagerCommands>,
    mut buf_stream: TcpStream,
    parent_rx: mpsc::Receiver<ParentalCommands>,
    cancellation_token_clone: CancellationToken,
    log_dir_path: String,
) {
    tracing::info!("Received a WriteToTopic command for topic: {}", topic_name);
    let (reply_tx, reply_rx) = oneshot::channel();
    topic_manager_tx_clone
        .send(TopicManagerCommands::GetTopicInfo {
            topic_name: topic_name.clone(),
            reply_tx,
        })
        .await
        .unwrap();

    match reply_rx.await.unwrap() {
        Some(topic) => {
            messages.iter().for_each(|message| {
                let partition = message.to_owned().key.unwrap_or_default();
            });
        }
        None => {
            tracing::error!("Topic {} does not exist", topic_name);
            let response = ClientResponses::TopicNotFound { topic: topic_name };
            let response_bytes = bincode::serialize(&response).unwrap();
            buf_stream.write_all(&response_bytes).await.unwrap();
            buf_stream.flush().await.unwrap();
            buf_stream.shutdown().await.unwrap();
        }
    }
}

async fn handle_create_topic_request(
    topic: Topic,
    topic_manager_tx_clone: mpsc::Sender<TopicManagerCommands>,
    mut buf_stream: BufStream<tokio::net::TcpStream>,
) {
    tracing::info!("Received a CreateTopic command: {:?}", topic);
    let (reply_tx, reply_rx) = oneshot::channel();
    topic_manager_tx_clone
        .send(TopicManagerCommands::CreateTopic { topic, reply_tx })
        .await
        .unwrap();
    let response = reply_rx.await.unwrap();
    let response_bytes = bincode::serialize(&response).unwrap();

    buf_stream.write_all(&response_bytes).await.unwrap();
    buf_stream.flush().await.unwrap();
    buf_stream.shutdown().await.unwrap();
}
