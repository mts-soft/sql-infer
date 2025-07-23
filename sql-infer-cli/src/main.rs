pub mod codegen;
mod commands;
pub mod config;
pub mod utils;

use std::error::Error;

use clap::*;
use commands::Generate;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use crate::commands::analyze::Analyze;

#[derive(Parser)]
#[command(name = "sql-infer", bin_name = "sql-infer")]
enum Command {
    Generate(Generate),
    Analyze(Analyze),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::ERROR)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let command = Command::parse();
    match command {
        Command::Generate(args) => args.run().await,
        Command::Analyze(analyze) => analyze.run().await,
    }
}
