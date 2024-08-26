use clap::{Parser, Subcommand};
use commands::create_topic;
use common::models::Topic;

mod commands;

fn main() {
    common::enable_tracing();
    let args = Arguments::parse();
    match args.command {
        Some(Commands::CreateTopic {
            partition_count,
            batch_size,
            replication_factor,
        }) => {
            let topic_to_create = Topic {
                name: args.topic_name,
                num_partitions: partition_count.unwrap_or(1),
                replication_factor: replication_factor.unwrap_or(1),
                retention_period: 1,
                batch_size: batch_size.unwrap_or(1),
            };
            create_topic(topic_to_create, args.broker_address);
        }
        Some(Commands::WriteToTopic { message }) => {
            tracing::info!("Writing message: {} to topic: {}", message, args.topic_name);
        }
        None => {
            tracing::info!("ERROR: No command provided");
        }
    }
}

#[derive(Debug, Parser, Default)]
struct Arguments {
    #[clap(subcommand)]
    command: Option<Commands>,

    #[clap(short = 'a', long = "broker-address")]
    broker_address: String,

    #[clap(short = 't', long = "topic-name")]
    topic_name: String,
}

#[derive(Debug, Subcommand)]
enum Commands {
    CreateTopic {
        #[clap(short = 'p')]
        partition_count: Option<u8>,

        #[clap(short = 'b')]
        batch_size: Option<u16>,

        #[clap(short = 'r')]
        replication_factor: Option<u8>,
    },
    WriteToTopic {
        #[clap(short = 'm')]
        message: String,
    },
}
