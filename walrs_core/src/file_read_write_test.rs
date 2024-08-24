use std::{time::SystemTime, vec};

use codecs::encoder::MessageEncoder;
use futures::{sink::SinkExt, StreamExt};
use models::Message;
use tokio::{fs::OpenOptions, io};
use tokio_util::codec::{FramedRead, FramedWrite};

mod codecs;
mod models;

#[tokio::main]
async fn file_io_main() -> io::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("kafka.wal")
        .await?;

    let mut file_writer = FramedWrite::new(
        file,
        MessageEncoder {
            payload_max_bytes: 10,
        },
    );

    let messages = vec![
        Message {
            offset: 0,
            payload: vec![1, 2, 3].into(),
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        },
        Message {
            offset: 1,
            payload: vec![11, 22, 33, 44, 55].into(),
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        },
    ];

    for message in messages {
        file_writer.send(message).await?;
    }

    file_writer.flush().await?;

    let wal_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("kafka.wal")
        .await?;

    let mut file_reader = FramedRead::new(wal_file, codecs::decoder::MessageDecoder {});
    while let Some(message) = file_reader.next().await {
        match message {
            Ok(message) => {
                println!("{:?}, payload: {:?}", message, message.payload.as_ref());
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
            }
        }
    }

    Ok(())
}
