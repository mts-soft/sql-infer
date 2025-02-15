use std::{
    error::Error,
    fs::{self, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
};

use async_std::task;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

fn init_debug() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

use crate::check_query::{check_query, query_to_sql_alchemy};
#[derive(clap::Args)]
#[command(version, about, long_about = None)]
pub struct CheckQueryArgs {
    #[arg(long, help = "DB connection URL")]
    db: String,
    #[arg(long, help = "The SQL query to check")]
    sql: std::path::PathBuf,
    #[arg(long, help = "Will output to the given file if provided.")]
    out: Option<std::path::PathBuf>,
    #[arg(
        long,
        help = "Enable the experimental parser to more accurately detect types"
    )]
    experimental_parser: bool,
    #[arg(long, help = "Show debug information")]
    debug: bool,
}

impl CheckQueryArgs {
    pub fn check_query(self) -> Result<(), Box<dyn Error>> {
        if self.debug {
            init_debug()?;
        }
        let file = OpenOptions::new().read(true).open(self.sql)?;
        let mut reader = BufReader::new(file);
        let mut query = String::new();
        reader.read_to_string(&mut query)?;
        let query_types = task::block_on(check_query(&self.db, &query, self.experimental_parser))?;
        eprintln!("Check successful!");
        eprintln!("Input types: ");
        for input in &query_types.input {
            eprintln!("{}: {}", input.name, input.type_name)
        }
        eprintln!("Output types: ");
        for output in &query_types.output {
            eprintln!("{}: {}", output.name, output.type_name)
        }
        Ok(())
    }
}

#[derive(clap::Args)]
#[command(version, about, long_about = None)]
pub struct CreateQueryArgs {
    #[arg(long, help = "DB connection URL")]
    db_url: String,
    #[arg(long, help = "The directory where all the queries are in")]
    sql_dir: std::path::PathBuf,
    #[arg(long, help = "Will output to the given file if provided.")]
    out: Option<std::path::PathBuf>,
    #[arg(
        long,
        help = "Enable the experimental parser to more accurately detect types"
    )]
    experimental_parser: bool,
    #[arg(long, help = "Show debug information")]
    debug: bool,
}

impl CreateQueryArgs {
    pub fn create_query(self) -> Result<(), Box<dyn Error>> {
        if self.debug {
            init_debug()?;
        }
        let directory = fs::read_dir(self.sql_dir)?;
        let mut query = String::new();
        let mut code = include_str!("./template.txt").to_string();
        for file in directory {
            query.clear();
            let file_path = file?.path();
            let Some(stem) = file_path.file_stem() else {
                info!("Skipping {file_path:?} as the filename is not valid.");
                continue;
            };
            let file_name = stem.to_string_lossy().to_string();

            let file = OpenOptions::new().read(true).open(file_path)?;
            let mut reader = BufReader::new(file);
            reader.read_to_string(&mut query)?;

            let check_result =
                task::block_on(check_query(&self.db_url, &query, self.experimental_parser));
            let query_types = match check_result {
                Ok(query_types) => query_types,
                Err(err) => {
                    error!("Check for {file_name} failed\n {err}");
                    continue;
                }
            };
            info!("Check for {file_name} successful!");
            code.push_str(&query_to_sql_alchemy(&file_name, &query, &query_types)?);
            code.push('\n');
        }
        if let Some(out) = self.out {
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(out)?;
            let mut writer = BufWriter::new(file);
            writer.write_all(code.as_bytes())?;
        } else {
            println!("{}", code);
        }
        Ok(())
    }
}
