use std::{
    io::{Read, Write},
    net::TcpStream,
};

use common::models::{Topic, TopicCommand};

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
