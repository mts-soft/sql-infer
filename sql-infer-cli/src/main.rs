pub mod codegen;
mod commands;
pub mod config;
pub mod utils;

use std::error::Error;

use clap::*;
use commands::Generate;

#[derive(Parser)]
#[command(name = "sql-infer", bin_name = "sql-infer")]
enum Command {
    Generate(Generate),
}

fn main() -> Result<(), Box<dyn Error>> {
    let command = Command::parse();
    match command {
        Command::Generate(args) => args.run(),
    }
}
