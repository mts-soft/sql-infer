use std::{
    error::Error,
    fs::{self, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
};

use async_std::task;

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
}

impl CheckQueryArgs {
    pub fn check_query(self) -> Result<(), Box<dyn Error>> {
        let file = OpenOptions::new().read(true).open(self.sql)?;
        let mut reader = BufReader::new(file);
        let mut query = String::new();
        reader.read_to_string(&mut query)?;

        let query_types = task::block_on(check_query(&self.db, &query))?;
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
}

impl CreateQueryArgs {
    pub fn create_query(self) -> Result<(), Box<dyn Error>> {
        let directory = fs::read_dir(self.sql_dir)?;
        let mut query = String::new();
        let mut code = include_str!("./template.txt").to_string();
        for file in directory {
            query.clear();
            let file_path = file?.path();
            let Some(stem) = file_path.file_stem() else {
                eprintln!("Skipping {file_path:?} as the filename is not valid.");
                continue;
            };
            let file_name = stem.to_string_lossy().to_string();

            let file = OpenOptions::new().read(true).open(file_path)?;
            let mut reader = BufReader::new(file);
            reader.read_to_string(&mut query)?;
            let query_types = task::block_on(check_query(&self.db_url, &query))?;
            eprintln!("Check for {file_name} successful!");
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
