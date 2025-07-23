pub mod codegen;
mod commands;
pub mod config;
pub mod utils;

use std::error::Error;

use clap::*;
use commands::Generate;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "sql-infer", bin_name = "sql-infer")]
enum Command {
    Generate(Generate),
}

fn main() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let command = Command::parse();
    match command {
        Command::Generate(args) => args.run(),
    }
}
