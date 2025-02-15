use sqlx::{postgres::PgPoolOptions, Executor};
use sqlx::{query, Column, Either, Pool, Postgres, Statement, TypeInfo};
use std::{error::Error, fmt};
use tracing::warn;

use crate::parser::{find_source, to_ast};
use crate::query_converter::prepare_dbapi2;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryItem {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFn {
    pub query: String,
    pub inputs: Vec<QueryItem>,
    pub outputs: Vec<QueryItem>,
}

#[derive(Debug, Clone)]
pub enum CheckerError {
    UnrecognizedType { sql_type: String },
}

impl fmt::Display for CheckerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CheckerError::UnrecognizedType { sql_type } => {
                write!(f, "Unrecognized SQL Type {}", sql_type)
            }
        }
    }
}

impl Error for CheckerError {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Nullability {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct QueryValue {
    pub name: String,
    pub type_name: String,
    pub nullable: Nullability,
}

#[derive(Debug, Clone)]
pub struct QueryTypes {
    pub input: Box<[QueryValue]>,
    pub output: Box<[QueryValue]>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SqlType {
    Bool,
    // Integer Types
    Int2,
    Int4,
    Int8,
    // Auto Increment Types
    SmallSerial,
    Serial,
    BigSerial,
    // Decimal types
    Decimal,
    // Time types
    Timestamp { tz: bool },
    Date,
    Time { tz: bool },
    // Text types
    Char { length: Option<u32> },
    VarChar { length: Option<u32> },
    Text,
    // Json types
    Json,
    Jsonb,
    // Float types
    Float4,
    Float8,
}

impl SqlType {
    fn from_str(sql_type: &str) -> Result<Self, Box<dyn Error>> {
        Ok(match sql_type {
            "BOOL" => Self::Bool,
            "SMALLINT" | "INT2" => Self::Int2,
            "INT" | "INT4" => Self::Int4,
            "INT8" => Self::Int8,
            "SMALLSERIAL" => Self::SmallSerial,
            "SERIAL" => Self::Serial,
            "BIGSERIAL" => Self::BigSerial,
            "NUMERIC" => Self::Decimal,
            "TIMESTAMP" => Self::Timestamp { tz: false },
            "TIMESTAMPTZ" => Self::Timestamp { tz: true },
            "TIME" | "TIMETZ" => Self::Time { tz: true },
            "DATE" => Self::Date,
            "CHAR" => Self::Char { length: None },
            "VARCHAR" => Self::VarChar { length: None },
            "TEXT" => Self::Text,
            "JSON" => Self::Json,
            "JSONB" => Self::Json,
            "DOUBLE PRECISION" | "FLOAT8" => Self::Float8,
            "REAL" | "FLOAT4" => Self::Float4,
            _ => Err(CheckerError::UnrecognizedType {
                sql_type: sql_type.to_string(),
            })?,
        })
    }
}

pub async fn get_column_nullability(
    pool: &Pool<Postgres>,
    table_name: &str,
    column_name: &str,
) -> Result<Nullability, Box<dyn Error>> {
    let query = query!(
        "select is_nullable from INFORMATION_SCHEMA.COLUMNS where table_name = $1 and column_name = $2;",
        table_name,
        column_name
    );
    let res = query.fetch_optional(pool).await?;
    let Some(column) = res else {
        return Ok(Nullability::Unknown);
    };
    Ok(column
        .is_nullable
        .map_or(Nullability::Unknown, |nullable| match nullable == "NO" {
            true => Nullability::False,
            false => Nullability::True,
        }))
}

pub async fn infer_nullability(
    pool: &Pool<Postgres>,
    query: &str,
    output_types: &mut [QueryValue],
) -> Result<(), Box<dyn Error>> {
    let ast = to_ast(query)?;
    let mut errors: Vec<String> = vec![];
    for output in output_types.iter_mut() {
        match find_source(&ast, &output.name) {
            Ok(Some(source)) => {
                let Some(table) = source.table else {
                    warn!("No source table found for column {}", &output.name);
                    continue;
                };
                if table.nullable {
                    output.nullable = Nullability::True;
                    continue;
                }
                let nullability = get_column_nullability(pool, &table.name, &output.name).await?;
                output.nullable = nullability;
            }
            Ok(None) => errors.push("No Sources Found".into()),
            Err(err) => errors.push(err.to_string()),
        }
    }
    for error in errors {
        warn!("{error}");
    }

    Ok(())
}

pub async fn check_query(
    db_url: &str,
    query: &str,
    experimental_parser: bool,
) -> Result<QueryTypes, Box<dyn Error>> {
    let prepared_query = prepare_dbapi2(query)?;
    let query = &prepared_query.postgres_query;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await?;
    let prepared = pool.prepare(query).await?;
    let mut result_types: Box<[QueryValue]> = prepared
        .columns()
        .iter()
        .map(|column| QueryValue {
            name: column.name().to_string(),
            type_name: column.type_info().name().to_owned(),
            nullable: Nullability::Unknown,
        })
        .collect();
    let parameters = match prepared.parameters() {
        Some(Either::Left(parameters)) => parameters
            .iter()
            .map(TypeInfo::name)
            .zip(prepared_query.params.into_iter())
            .map(|(type_name, name)| QueryValue {
                name,
                type_name: type_name.to_owned(),
                nullable: Nullability::Unknown,
            })
            .collect(),
        Some(Either::Right(_)) => panic!("Postgres connection should never lead here"),
        None => panic!("Parameter types were not provided."),
    };
    if experimental_parser {
        infer_nullability(&pool, query, &mut result_types).await?;
    }
    pool.close().await;

    Ok(QueryTypes {
        input: parameters,
        output: result_types,
    })
}

pub fn query_to_json(query: &str, query_types: &QueryTypes) -> Result<QueryFn, Box<dyn Error>> {
    let mut input_types = Vec::with_capacity(query_types.input.len());
    for input in &query_types.input {
        input_types.push(QueryItem {
            name: input.name.clone(),
            sql_type: SqlType::from_str(&input.type_name)?,
            nullable: input.nullable != Nullability::False,
        });
    }
    let mut output_types = Vec::with_capacity(query_types.output.len());
    for output in &query_types.output {
        output_types.push(QueryItem {
            name: output.name.clone(),
            sql_type: SqlType::from_str(&output.type_name)?,
            nullable: output.nullable != Nullability::False,
        });
    }
    Ok(QueryFn {
        query: query.to_string(),
        inputs: input_types,
        outputs: output_types,
    })
}
