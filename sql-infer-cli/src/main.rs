pub mod codegen;
mod commands;
pub mod config;
pub mod schema;
pub mod utils;

use clap::*;
use commands::Generate;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use crate::commands::{analyze::Analyze, schema::Schema};

#[derive(Parser)]
#[command(name = "sql-infer", bin_name = "sql-infer")]
enum Command {
    Generate(Generate),
    Analyze(Analyze),
    Schema(Schema),
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::ERROR)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let command = Command::parse();
    let res = match command {
        Command::Generate(args) => args.run().await,
        Command::Analyze(analyze) => analyze.run().await,
        Command::Schema(schema) => schema.run().await,
    };
    if let Err(err) = res {
        return Err(err.to_string());
    }
    Ok(())
}
