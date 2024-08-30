use std::{
    io::{Read, Write},
    net::TcpStream,
};

use common::models::{Message, Topic, TopicCommand};

pub fn create_topic(topic: Topic, broker_address: String) {
    tracing::info!("Creating topic: {:?} on broker: {}", topic, broker_address);
    let mut stream = TcpStream::connect(broker_address).expect("Could not connect to broker");

    let topic_bytes = bincode::serialize(&TopicCommand::CreateTopic { topic }).unwrap();
    stream
        .write_all(&topic_bytes)
        .expect("Could not write to stream");
    tracing::info!("Request to create topic sent to server.");

    let mut buffer = Vec::new();
    stream
        .read_to_end(&mut buffer)
        .expect("Could not read from stream");

    let response = String::from_utf8_lossy(&buffer);
    tracing::info!("Response from server: {:?}", response);
}

pub fn write_message(message: String, topic_name: String, broker_address: String) {
    tracing::info!(
        "Writing message to topic: {} on broker: {}",
        topic_name,
        broker_address
    );
    let mut stream = TcpStream::connect(broker_address).expect("Could not connect to broker");

    let message = Message::new(message.into(), None, None);

    let topic_bytes = bincode::serialize(&TopicCommand::WriteToTopic {
        topic_name,
        messages: vec![message],
    })
    .unwrap();

    stream
        .write_all(&topic_bytes)
        .expect("Could not write to stream");

    tracing::info!("Request to write message sent to server.");
}
