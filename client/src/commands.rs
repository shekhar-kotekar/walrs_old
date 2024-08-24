use std::{
    io::{Read, Write},
    net::TcpStream,
};

use common::models::Topic;

pub fn create_topic(topic: Topic, broker_address: String) {
    println!(
        "INFO: Creating topic: {:?} on broker: {}",
        topic, broker_address
    );
    let mut stream = TcpStream::connect(broker_address).expect("Could not connect to broker");
    let topic_bytes = bincode::serialize(&topic).unwrap();
    stream
        .write_all(&topic_bytes)
        .expect("Could not write to stream");
    println!("INFO: Request to create topic sent to server.");

    let mut buffer = [0; 1024];
    stream
        .read(&mut buffer)
        .expect("Could not read from stream");
    let response = String::from_utf8_lossy(&buffer);
    println!("INFO: Response from server: {:?}", response);
}