# The Great Wal-RS
Kafka implementation using Rust and Tokio.

## Binned repo
This repo has some useful code but discarding it for various reasons, attempting it one more time as part of another repository.

## Console client
Create a new topic using below command:
```
cargo run --package client -- --broker-address localhost:30002 --topic-name <TOPIC NAME> create-topic
```
## Roadmap
### Kafka features to implement
We will implement below mentioned features one by one. We can track the progress via GitHub issues.
- Kafka consumer
- Segment changes in Partition
    - As of now each partition is writing messages in a single file whereas Kafka writes certain number of messages in a single file (aka segment) and changes the segment after file size goes above certain threshold. We need to implement similar functionality.
- Log compaction feature
- Broker election
- Partition sync across different nodes / racks / data centres.
    - Kafka uses "Distributed logs" mechanism to replicate messages across different brokers. We need to implement similar feature.
- Message encryption
- APIs for different languages
    - Java
    - Python
    - Go
