use std::{
    collections::HashSet,
    error::Error,
    fs::OpenOptions,
    io::{BufReader, Read},
    path::PathBuf,
};

use clap::Parser;
use sql_infer_core::{
    SqlInferBuilder,
    inference::{
        QueryItem,
        datatypes::{DecimalPrecision, TextLength},
        nullability::ColumnNullability,
    },
};
use sqlx::postgres::PgPoolOptions;

use crate::{
    codegen::{
        CodeGen, QueryDefinition, json::JsonCodeGen, sqlalchemy::SqlAlchemyCodeGen,
        sqlalchemy_async::SqlAlchemyAsyncCodeGen,
    },
    config::{CodeGenerator, SqlInferConfig, TomlConfig},
    utils::{ParametrizedQuery, parse_into_postgres},
};

#[derive(Parser, Debug, Clone)]
#[must_use]
pub struct Generate {
    config: Option<PathBuf>,
}

impl Generate {
    pub fn run(self) -> Result<(), Box<dyn Error>> {
        let config = match self.config {
            Some(config) => config,
            None => PathBuf::from("sql-infer.toml"),
        };
        let config: TomlConfig = toml::from_slice(&std::fs::read(config)?)?;
        let config: SqlInferConfig = SqlInferConfig::from_toml_config(config)?;

        let mut sql_infer = SqlInferBuilder::default();
        if config.experimental_features.nullability() {
            sql_infer.add_information_schema_pass(ColumnNullability);
        }
        if config.experimental_features.decimal_precision() {
            sql_infer.add_information_schema_pass(DecimalPrecision);
        }
        if config.experimental_features.text_length() {
            sql_infer.add_information_schema_pass(TextLength);
        }
        let sql_infer = sql_infer.build();

        let mut codegen: Box<dyn CodeGen> = match config.mode {
            CodeGenerator::Json => Box::new(JsonCodeGen::default()),
            CodeGenerator::SqlAlchemy => Box::new(SqlAlchemyCodeGen::default()),
            CodeGenerator::SqlAlchemyAsync => Box::new(SqlAlchemyAsyncCodeGen::default()),
        };

        let rt = tokio::runtime::Runtime::new()?;
        let pool = rt.block_on(
            PgPoolOptions::new()
                .max_connections(1)
                .connect(&config.database_url),
        )?;

        let mut query = String::new();
        let mut files = HashSet::<String>::new();

        for directory in config.source {
            for file in std::fs::read_dir(directory)? {
                let file = file?;
                if !file.metadata()?.is_file() {
                    continue;
                }
                let file_path = file.path();
                let Some(stem) = file_path.file_stem() else {
                    tracing::info!("Skipping {file_path:?} as the filename is not valid.");
                    continue;
                };
                query.clear();
                let file_name = stem.to_string_lossy().to_string();

                let file = OpenOptions::new().read(true).open(file_path)?;
                let mut reader = BufReader::new(file);
                reader.read_to_string(&mut query)?;

                let ParametrizedQuery { raw_query, params } = parse_into_postgres(&query)?;

                let check_result = rt.block_on(sql_infer.infer_types(&pool, &raw_query));
                let query_types = match check_result {
                    Ok(query_types) => query_types,
                    Err(err) => {
                        tracing::error!("Check for {file_name} failed\n {err}");
                        continue;
                    }
                };
                tracing::info!("Check for {file_name} successful!");
                if files.contains(&file_name) {
                    tracing::error!("{file_name} already exists. Skipping...");
                    continue;
                }
                let query = QueryDefinition {
                    query: query.clone(),
                    inputs: query_types
                        .input
                        .into_iter()
                        .zip(params)
                        .map(|(item, param_name)| QueryItem {
                            name: param_name,
                            sql_type: item.sql_type,
                            nullable: item.nullable,
                        })
                        .collect(),
                    outputs: query_types.output,
                };
                codegen.push(&file_name, query)?;
                files.insert(file_name);
            }
        }
        let code = codegen.finalize()?;
        std::fs::write(config.target, code)?;
        Ok(())
    }
}
