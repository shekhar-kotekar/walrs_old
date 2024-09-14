use clap::{Parser, Subcommand};
use commands::{create_topic, write_message};
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
                num_partitions: partition_count,
                replication_factor,
                retention_period: Some(1),
                batch_size,
            };
            create_topic(topic_to_create, args.broker_address);
        }
        Some(Commands::WriteToTopic { message }) => {
            write_message(message, args.topic_name, args.broker_address)
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
        batch_size: Option<u8>,

        #[clap(short = 'r')]
        replication_factor: Option<u8>,
    },
    WriteToTopic {
        #[clap(short = 'm')]
        message: String,
    },
}
