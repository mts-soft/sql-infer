use std::{error::Error, path::PathBuf};

use clap::{Parser, ValueEnum};
use sql_infer_core::{
    SqlInferBuilder,
    inference::{
        Nullability,
        datatypes::{DecimalPrecision, TextLength},
        nullability::ColumnNullability,
    },
};
use sqlx::{postgres::PgPoolOptions, query};

use crate::{
    config::{self, SqlInferConfig, TomlConfig},
    schema::{self, ColumnSchema, DbSchema, TableSchema, lint::Lint},
};

#[derive(ValueEnum, Debug, Clone, Default)]
pub enum Analysis {
    #[default]
    Display,
    Lint,
}

#[derive(Parser, Debug, Clone)]
#[must_use]
pub struct Schema {
    analysis: Analysis,
    config: Option<PathBuf>,
}

impl Schema {
    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        // FIXME: Duplicate code
        let config = match self.config {
            Some(config) => config,
            None => PathBuf::from("sql-infer.toml"),
        };
        let config: TomlConfig = toml::from_slice(&std::fs::read(&config).map_err(|error| {
            format!(
                "encountered '{error}' attempting to read {}",
                config.display()
            )
        })?)?;
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

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&config::db_url()?)
            .await?;
        let tables = query!(
            r#"SELECT
    table_name
FROM
    information_schema.tables
WHERE
    table_schema NOT IN ('pg_catalog', 'information_schema')"#
        )
        .fetch_all(&pool)
        .await?;
        let tables: Vec<_> = tables
            .into_iter()
            .flat_map(|record| record.table_name)
            .collect();

        let mut table_schemas = vec![];
        for table in tables {
            // Guaranteed to be valid table name, escape double quotes with double quotes as per PostgreSQL documentation.
            let table = table.replace("\"", "\"\"");
            let types = sql_infer
                .infer_types(&pool, &format!("select * from {table}"))
                .await?;
            let mut columns = vec![];
            for col in types.output {
                columns.push(ColumnSchema {
                    name: col.name,
                    data_type: col.sql_type,
                    nullable: col.nullable == Nullability::True,
                });
            }
            table_schemas.push(TableSchema {
                name: table,
                columns,
            });
        }
        let db_schema = DbSchema {
            tables: table_schemas,
        };

        match self.analysis {
            Analysis::Display => {
                println!("{db_schema}");
            }
            Analysis::Lint => {
                let ttz = schema::lint::TimeWithTimezone;
                let twt = schema::lint::TimestampWithoutTimezone;
                let tcnc = schema::lint::TableColumnNameClash;
                for error in ttz.lint(&db_schema) {
                    println!("{error}");
                }
                for error in twt.lint(&db_schema) {
                    println!("{error}");
                }
                for error in tcnc.lint(&db_schema) {
                    println!("{error}");
                }
            }
        }
        Ok(())
    }
}
