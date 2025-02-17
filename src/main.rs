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
use config::{SqlInferConfig, SqlInferOptions};

#[derive(Parser)]
#[command(name = "sql-infer")]
#[command(bin_name = "sql-infer")]
enum Command {
    Generate(Generate),
    Init(Initialize),
}

fn get_config() -> Result<SqlInferConfig, Box<dyn Error>> {
    let content = std::fs::read_to_string("sql-infer.toml")?;
    let options: SqlInferOptions = toml::from_str(&content)?;
    Ok(options.into_config())
}

fn main() -> Result<(), Box<dyn Error>> {
    let command = Command::parse();
    match command {
        Command::Generate(args) => args.generate(get_config()?),
        Command::Init(args) => args.init(),
    }
}
