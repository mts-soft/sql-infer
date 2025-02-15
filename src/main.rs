#[deny(clippy::unwrap_used)]
mod check_query;
mod commands;
mod query_converter;
mod utils;
mod parser;

use std::{
    error::Error,
    process::exit,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use clap::*;
use commands::{CheckQueryArgs, CreateQueryArgs};

#[derive(Parser)]
#[command(name = "sql-py")]
#[command(bin_name = "sql-py")]
#[non_exhaustive]
enum Command {
    CheckQuery(CheckQueryArgs),
    CreateQuery(CreateQueryArgs),
}

fn main() -> Result<(), Box<dyn Error>> {
    let quit_handler = Arc::new(AtomicBool::new(false));
    let ctrl_c_handler = quit_handler.clone();
    ctrlc::set_handler(move || {
        let already_quit = ctrl_c_handler.swap(true, Ordering::Relaxed);
        if already_quit {
            eprintln!("Force exit.");
            exit(1);
        }
        eprintln!("Waiting for graceful exit...");
    })?;
    let command = Command::parse();
    match command {
        Command::CheckQuery(args) => args.check_query(),
        Command::CreateQuery(args) => args.create_query(),
    }
}
