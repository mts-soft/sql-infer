use sqlx::{postgres::PgPoolOptions, Executor};
use sqlx::{query, Column, Either, Pool, Postgres, Statement, TypeInfo};
use std::fmt::Display;
use std::{error::Error, fmt};
use tracing::warn;

use crate::config::FeatureSet;
use crate::parser::{find_source, to_ast, DbTable};
use crate::query_converter::prepare_dbapi2;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryItem {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: Nullability,
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Nullability {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTypes {
    pub input: Box<[QueryItem]>,
    pub output: Box<[QueryItem]>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
    Decimal {
        precision: Option<u32>,
        precision_radix: Option<u32>,
    },
    // Time types
    Timestamp {
        tz: bool,
    },
    Date,
    Time {
        tz: bool,
    },
    Interval,
    // Text types
    Char {
        length: Option<u32>,
    },
    VarChar {
        length: Option<u32>,
    },
    Text,
    // Json types
    Json,
    Jsonb,
    // Float types
    Float4,
    Float8,
}

impl Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlType::Bool => write!(f, "boolean"),
            SqlType::Int2 => write!(f, "i16"),
            SqlType::Int4 => write!(f, "i32"),
            SqlType::Int8 => write!(f, "i64"),
            SqlType::SmallSerial => write!(f, "i16"),
            SqlType::Serial => write!(f, "i32"),
            SqlType::BigSerial => write!(f, "i64"),
            SqlType::Decimal {
                precision,
                precision_radix,
            } => {
                let precision = if let Some(precision) = precision {
                    &format!("{precision}")
                } else {
                    "???"
                };
                let precision_radix = if let Some(precision_radix) = precision_radix {
                    &format!("{precision_radix}")
                } else {
                    "???"
                };
                write!(f, "decimal({}, {})", precision, precision_radix)
            }
            SqlType::Timestamp { tz } => write!(
                f,
                "timestamp {}",
                if *tz {
                    "with timezone"
                } else {
                    "without timezone"
                }
            ),
            SqlType::Date => write!(f, "date"),
            SqlType::Time { tz } => write!(
                f,
                "time {}",
                if *tz {
                    "with timezone"
                } else {
                    "without timezone"
                }
            ),
            SqlType::Interval => write!(f, "interval"),
            SqlType::Char { length } => {
                let length = if let Some(length) = length {
                    &format!("{length}")
                } else {
                    "???"
                };
                write!(f, "char({length})",)
            }
            SqlType::VarChar { length } => {
                let length = if let Some(length) = length {
                    &format!("{length}")
                } else {
                    "???"
                };
                write!(f, "varchar({length})",)
            }
            SqlType::Text => write!(f, "text"),
            SqlType::Json => write!(f, "json"),
            SqlType::Jsonb => write!(f, "jsonb"),
            SqlType::Float4 => write!(f, "f32"),
            SqlType::Float8 => write!(f, "f64"),
        }
    }
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
            "NUMERIC" => Self::Decimal {
                precision: None,
                precision_radix: None,
            },
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
            "INTERVAL" => Self::Interval,
            _ => Err(CheckerError::UnrecognizedType {
                sql_type: sql_type.to_string(),
            })?,
        })
    }
}

pub async fn update_with_info(
    pool: &Pool<Postgres>,
    table: &DbTable,
    item: &mut QueryItem,
    features: &FeatureSet,
) -> Result<(), Box<dyn Error>> {
    let query = query!(
        "select is_nullable, character_maximum_length, numeric_precision, numeric_precision_radix from INFORMATION_SCHEMA.COLUMNS where table_name = $1 and column_name = $2;",
        table.name,
        item.name,
    );
    let res = query.fetch_optional(pool).await?;
    let Some(column) = res else {
        return Ok(());
    };
    if features.precise_output_datatypes {
        if let SqlType::Char { length } | SqlType::VarChar { length } = &mut item.sql_type {
            if let Some(character_maximum_length) = column.character_maximum_length {
                *length = Some(character_maximum_length as u32)
            }
        }
        if let SqlType::Decimal {
            precision,
            precision_radix,
        } = &mut item.sql_type
        {
            if let Some((numeric_precision, numeric_precision_radix)) =
                column.numeric_precision.zip(column.numeric_precision_radix)
            {
                *precision = Some(numeric_precision as u32);
                *precision_radix = Some(numeric_precision_radix as u32);
            };
        }
    }

    if features.infer_nullability {
        if table.nullable {
            item.nullable = Nullability::True;
        } else {
            item.nullable = match column.is_nullable {
                Some(nullable) => match &*nullable {
                    "NO" => Nullability::False,
                    "YES" => Nullability::True,
                    _ => Nullability::Unknown,
                },
                None => Nullability::Unknown,
            };
        }
    }
    Ok(())
}

pub async fn feature_passes(
    pool: &Pool<Postgres>,
    query: &str,
    output_types: &mut [QueryItem],
    features: &FeatureSet,
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
                update_with_info(pool, &table, output, features).await?;
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
    features: &FeatureSet,
) -> Result<QueryTypes, Box<dyn Error>> {
    let mut in_quotes = false;
    let mut query_buffer = String::with_capacity(query.len());

    let mut input_types = vec![];
    let mut output_types = None;
    for char in query.chars() {
        if char == '"' {
            in_quotes = !in_quotes
        }
        query_buffer.push(char);
        if !in_quotes && char == ';' {
            let query = check_statement(db_url, &query_buffer, features).await?;
            query_buffer.clear();
            for input in query.input {
                if input_types.contains(&input) {
                    continue;
                }
                input_types.push(input);
            }
            output_types = Some(query.output);
        }
    }
    match output_types {
        Some(output_types) => Ok(QueryTypes {
            input: input_types.into_boxed_slice(),
            output: output_types,
        }),
        None => check_statement(db_url, query, features).await,
    }
}

async fn check_statement(
    db_url: &str,
    query: &str,
    features: &FeatureSet,
) -> Result<QueryTypes, Box<dyn Error>> {
    let prepared_query = prepare_dbapi2(query)?;
    let query = &prepared_query.postgres_query;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await?;
    let prepared = pool.prepare(query).await?;
    let mut result_types = Vec::with_capacity(prepared.columns().len());
    for column in prepared.columns() {
        result_types.push(QueryItem {
            name: column.name().to_string(),
            sql_type: SqlType::from_str(column.type_info().name())?,
            nullable: Nullability::Unknown,
        });
    }
    let mut input_types = vec![];
    match prepared.parameters() {
        Some(Either::Left(parameters)) => {
            for (param, name) in parameters.iter().zip(prepared_query.params.iter()) {
                input_types.push(QueryItem {
                    name: name.to_string(),
                    sql_type: SqlType::from_str(param.name())?,
                    nullable: Nullability::Unknown,
                });
            }
        }
        Some(Either::Right(_)) => panic!("Postgres connection should never lead here"),
        None => panic!("Parameter types were not provided."),
    };
    feature_passes(&pool, query, &mut result_types, features).await?;
    pool.close().await;

    Ok(QueryTypes {
        input: input_types.into_boxed_slice(),
        output: result_types.into_boxed_slice(),
    })
}

pub fn to_query_fn(query: &str, query_types: &QueryTypes) -> Result<QueryFn, Box<dyn Error>> {
    let mut input_types = Vec::with_capacity(query_types.input.len());
    for input in &query_types.input {
        input_types.push(QueryItem {
            name: input.name.clone(),
            sql_type: input.sql_type,
            nullable: input.nullable,
        });
    }
    let mut output_types = Vec::with_capacity(query_types.output.len());
    for output in &query_types.output {
        output_types.push(QueryItem {
            name: output.name.clone(),
            sql_type: output.sql_type,
            nullable: output.nullable,
        });
    }
    Ok(QueryFn {
        query: query.to_string(),
        inputs: input_types,
        outputs: output_types,
    })
}
