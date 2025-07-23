use std::error::Error;

use clap::{Parser, ValueEnum};
use sql_infer_core::{
    inference::{self},
    parser,
};
use sqlx::postgres::PgPoolOptions;

use crate::config;

#[derive(ValueEnum, Debug, Clone, Default)]
pub enum Analysis {
    #[default]
    Columns,
    ColumnsWithDb,
    Tables,
}

#[derive(Parser, Debug, Clone)]
#[must_use]
pub struct Analyze {
    analysis: Analysis,
    query: Vec<String>,
}

impl Analyze {
    fn get_query(query: String) -> Result<String, Box<dyn Error>> {
        Ok(match std::fs::exists(&query)? {
            true => std::fs::read_to_string(query)?,
            false => query,
        })
    }

    pub async fn run(self) -> Result<(), Box<dyn Error>> {
        for query in self.query {
            let query = &Self::get_query(query)?;
            let statements = parser::to_ast(query)?;
            match self.analysis {
                Analysis::Columns => {
                    for statement in statements {
                        let fields = parser::find_fields(&statement)?;
                        for (field, column) in fields {
                            println!("{field}: {column}");
                        }
                    }
                }
                Analysis::Tables => {
                    for statement in statements {
                        let tables = parser::find_tables(&statement);
                        for table in tables {
                            println!("{table}");
                        }
                    }
                }
                Analysis::ColumnsWithDb => {
                    let pool = PgPoolOptions::new()
                        .max_connections(1)
                        .connect(&config::db_url()?)
                        .await?;
                    for statement in statements {
                        let fields = parser::find_fields(&statement)?;
                        for (field, column) in fields {
                            let (column, _) =
                                inference::get_column_information_schema(&pool, &column).await?;
                            println!("{field}: {column}");
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
