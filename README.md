# The Great Wal-RS
Kafka implementation using Rust and Tokio.
## Console client
Create a new topic using below command:
```
cargo run --package client -- --broker-address localhost:30002 --topic-name <TOPIC NAME> create-topic
```
