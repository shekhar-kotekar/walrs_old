use common::models::Message;
use tokio::sync::mpsc::{Receiver, Sender};

use tokio_util::sync::CancellationToken;

pub enum ConsumerManagerCommand {
    ReadMessage { topic_name: String },
}

pub async fn start_consumer_manager(
    mut consumer_manager_rx: Receiver<ConsumerManagerCommand>,
    cancellation_token: CancellationToken,
) {
    let partition_readers = Vec::<Sender<Message>>::new();
    loop {
        tokio::select! {
            Some(consumer_manager_command) = consumer_manager_rx.recv() => {
                match consumer_manager_command {
                    ConsumerManagerCommand::ReadMessage { topic_name } => {
                        tracing::info!("Sending message to topic: {}", topic_name);
                    }
                }
            }
            _ = cancellation_token.cancelled() => {
                tracing::info!("breaking out of consumer manager");
                break;
            }
        }
    }
}
