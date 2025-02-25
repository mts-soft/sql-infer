#[deny(clippy::unwrap_used)]
mod check_query;
mod codegen;
mod commands;
mod config;
mod parser;
mod query_converter;

use std::error::Error;

use clap::*;
use commands::{Generate, Initialize};

#[derive(Parser)]
#[command(name = "sql-infer", bin_name = "sql-infer")]
enum Command {
    Generate(Generate),
    Init(Initialize),
}

fn main() -> Result<(), Box<dyn Error>> {
    let command = Command::parse();
    match command {
        Command::Generate(args) => args.generate(),
        Command::Init(args) => args.init(),
    }
}
