#[deny(clippy::unwrap_used)]
mod check_query;
mod commands;
mod parser;
mod query_converter;
mod sqlalchemy;

use std::error::Error;

use clap::*;
use commands::{CheckQueryArgs, CreateQueryArgs, SqlAlchemyArgs};

#[derive(Parser)]
#[command(name = "sql-py")]
#[command(bin_name = "sql-py")]
#[non_exhaustive]
enum Command {
    CheckQuery(CheckQueryArgs),
    CreateQuery(CreateQueryArgs),
    SqlAlchemy(SqlAlchemyArgs),
}

fn main() -> Result<(), Box<dyn Error>> {
    let command = Command::parse();
    match command {
        Command::CheckQuery(args) => args.check_query(),
        Command::CreateQuery(args) => args.create_query(),
        Command::SqlAlchemy(args) => args.generate_code(),
    }
}
