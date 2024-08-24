use clap::{Parser, Subcommand};
use commands::create_topic;
use common::models::Topic;

mod commands;

fn main() {
    let args = Arguments::parse();
    println!("{:?}", args);
    match args.command {
        Some(Commands::CreateTopic {
            broker_address,
            topic_name,
            partition_count,
            batch_size,
            replication_factor,
        }) => {
            let topic_to_create = Topic {
                name: topic_name,
                num_partitions: partition_count.unwrap_or(1),
                replication_factor: replication_factor.unwrap_or(1),
                retention_period: 1,
                batch_size: batch_size.unwrap_or(1),
            };
            create_topic(topic_to_create, broker_address);
        }
        None => {
            println!("No command provided");
        }
    }
}

#[derive(Debug, Parser, Default)]
struct Arguments {
    #[clap(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    CreateTopic {
        #[clap(short = 'a', long)]
        broker_address: String,

        #[clap(short = 't')]
        topic_name: String,

        #[clap(short = 'p')]
        partition_count: Option<u8>,

        #[clap(short = 'b')]
        batch_size: Option<u16>,

        #[clap(short = 'r')]
        replication_factor: Option<u8>,
    },
}
