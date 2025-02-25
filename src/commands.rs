use std::{
    collections::HashSet,
    error::Error,
    fs::{self, OpenOptions},
    io::{BufReader, Read},
};

use async_std::task;
use sqlx::postgres::PgPoolOptions;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

fn init_standard() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

fn init_debug() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

use crate::{
    check_query::{check_query, to_query_fn},
    codegen::{CodeGen, json::JsonCodeGen, sqlalchemy::SqlAlchemyCodeGen},
    config::{CodeGenOptions, ExperimentalFeatures, QueryPath, SqlInferOptions, get_config},
};

#[derive(clap::Args)]
#[command(about, long_about = None, name = "init")]
pub struct Initialize {}

impl Initialize {
    pub fn init(self) -> Result<(), Box<dyn Error>> {
        const FILE_NAME: &str = "sql-infer.toml";
        let exists = std::fs::exists(FILE_NAME)?;
        if exists {
            eprintln!("{FILE_NAME} already exists.\nExiting...");
            return Ok(());
        }

        let options = SqlInferOptions {
            path: QueryPath::Single("<path/to/input/directory>".into()),
            target: Some("<path/to/output/file>".into()),
            mode: CodeGenOptions::Json,
            database: None,
            experimental_features: ExperimentalFeatures::default(),
        };
        let toml = toml::to_string_pretty(&options)?;
        std::fs::write(FILE_NAME, toml)?;
        eprintln!("Written config to {FILE_NAME}!");
        Ok(())
    }
}

#[derive(clap::Args)]
#[command(about, long_about = None)]
pub struct Generate {
    #[arg(long, help = "Show debug information")]
    debug: bool,
}

impl Generate {
    pub fn generate(self) -> Result<(), Box<dyn Error>> {
        match self.debug {
            true => init_debug()?,
            false => init_standard()?,
        }
        let config = get_config()?;

        let paths = match config.path {
            QueryPath::Single(path) => fs::read_dir(path)?.collect::<Vec<_>>(),
            QueryPath::List(paths) => {
                let mut all_paths = vec![];
                for path in paths {
                    all_paths.extend(fs::read_dir(path)?);
                }
                all_paths
            }
        };
        let mut query = String::new();
        let mut files = HashSet::<String>::new();

        let mut codegen: Box<dyn CodeGen> = match config.mode {
            CodeGenOptions::Json => Box::new(JsonCodeGen::new()),
            CodeGenOptions::SqlAlchemy => Box::new(SqlAlchemyCodeGen::new()),
        };

        let pool = task::block_on(
            PgPoolOptions::new()
                .max_connections(1)
                .connect(&config.db_url),
        )?;

        for file in paths {
            let file = file?;
            if !file.metadata()?.is_file() {
                continue;
            }
            let file_path = file.path();
            let Some(stem) = file_path.file_stem() else {
                info!("Skipping {file_path:?} as the filename is not valid.");
                continue;
            };
            query.clear();
            let file_name = stem.to_string_lossy().to_string();

            let file = OpenOptions::new().read(true).open(file_path)?;
            let mut reader = BufReader::new(file);
            reader.read_to_string(&mut query)?;

            let check_result = task::block_on(check_query(&pool, &query, &config.features));
            let query_types = match check_result {
                Ok(query_types) => query_types,
                Err(err) => {
                    error!("Check for {file_name} failed\n {err}");
                    continue;
                }
            };
            info!("Check for {file_name} successful!");
            if files.contains(&file_name) {
                error!("{file_name} already exists. Skipping...");
                continue;
            }
            codegen.push(&file_name, to_query_fn(&query, &query_types)?)?;
            files.insert(file_name);
        }
        let code = codegen.finalize(&config.features)?;
        if let Some(out_file) = config.target {
            std::fs::write(out_file, code)?;
        } else {
            println!("{code}");
        }
        Ok(())
    }
}
