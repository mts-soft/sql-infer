pub mod datatypes;
pub mod nullability;

use serde::{Deserialize, Serialize};
use sqlx::{Column, Either, Pool, Postgres, Statement, TypeInfo};
use sqlx::{Executor, query_as};
use std::fmt::Display;
use std::{error::Error, fmt};

use crate::parser::{DbTable, find_source, to_ast};
use tracing::warn;

pub trait UseInformationSchema {
    fn apply(&self, schema: &InformationSchema, table: &mut DbTable, column: &mut QueryItem);
}

pub struct Passes {
    pub information_schema: Vec<Box<dyn UseInformationSchema>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlQuery {
    pub query: String,
    pub parameters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryItem {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: Nullability,
}

#[derive(Debug, Clone)]
pub enum CheckerError {
    UnrecognizedType { sql_type: String },
}

impl fmt::Display for CheckerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CheckerError::UnrecognizedType { sql_type } => {
                write!(f, "Unrecognized SQL Type {sql_type}")
            }
        }
    }
}

impl Error for CheckerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Nullability {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryTypes {
    pub input: Box<[QueryItem]>,
    pub output: Box<[QueryItem]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    Bit {
        length: Option<u32>,
    },
    VarBit {
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
            SqlType::Bool => write!(f, "bool"),
            SqlType::Int2 => write!(f, "i16"),
            SqlType::Int4 => write!(f, "i32"),
            SqlType::Int8 => write!(f, "i64"),
            SqlType::SmallSerial => write!(f, "i16"),
            SqlType::Serial => write!(f, "i32"),
            SqlType::BigSerial => write!(f, "i64"),
            SqlType::Decimal {
                precision,
                precision_radix,
            } => match precision.zip(precision_radix.as_ref()) {
                Some((precision, precision_radix)) => {
                    write!(f, "decimal({precision}, {precision_radix})")
                }
                None => write!(f, "decimal"),
            },
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
            SqlType::Bit { length } => write!(f, "bit({})", length.unwrap_or(1)),
            SqlType::VarBit {
                length: Some(length),
            } => write!(f, "varbit({length})"),
            SqlType::VarBit { length: None } => write!(f, "varbit"),
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
            "BIT" => Self::Char { length: None },
            "VARBIT" => Self::VarChar { length: None },
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

pub struct InformationSchema {
    pub is_nullable: Option<bool>,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub numeric_precision_radix: Option<i32>,
    pub numeric_scale: Option<i32>,
    pub column_default: Option<String>,
}

pub(crate) async fn update_with_info(
    pool: &Pool<Postgres>,
    table: &mut DbTable,
    item: &mut QueryItem,
    passes: &Passes,
) -> Result<(), Box<dyn Error>> {
    let query = query_as!(
        InformationSchema,
        "select
    (is_nullable = 'YES') as is_nullable,
    character_maximum_length,
    numeric_precision,
    numeric_precision_radix,
    numeric_scale,
    column_default
from
    INFORMATION_SCHEMA.COLUMNS
where
    table_name = $1
    and column_name = $2;",
        table.name,
        item.name,
    );
    let res = query.fetch_optional(pool).await?;
    let Some(information_schema) = res else {
        return Ok(());
    };
    for pass in &passes.information_schema {
        pass.apply(&information_schema, table, item);
    }
    Ok(())
}

pub(crate) async fn apply_passes(
    pool: &Pool<Postgres>,
    query: &str,
    output_types: &mut [QueryItem],
    passes: &Passes,
) -> Result<(), Box<dyn Error>> {
    let ast = to_ast(query)?;
    let mut errors: Vec<String> = vec![];
    for output in output_types.iter_mut() {
        match find_source(&ast, &output.name) {
            Ok(Some(source)) => {
                let Some(mut table) = source.table else {
                    warn!("No source table found for column {}", &output.name);
                    continue;
                };
                update_with_info(pool, &mut table, output, passes).await?;
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

pub(crate) async fn check_statement(
    pool: &Pool<Postgres>,
    query: &str,
    passes: &Passes,
) -> Result<QueryTypes, Box<dyn Error>> {
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
            for (param, name) in parameters.iter().zip(parameters.iter()) {
                input_types.push(QueryItem {
                    name: name.to_string(),
                    sql_type: SqlType::from_str(param.name())?,
                    nullable: Nullability::Unknown,
                });
            }
        }
        /*
        PgStatement::<'_>::parameters is defined as following:
        Some(Either::Left(&self.metadata.parameters))
        */
        _ => unreachable!(),
    };
    apply_passes(pool, query, &mut result_types, passes).await?;

    Ok(QueryTypes {
        input: input_types.into_boxed_slice(),
        output: result_types.into_boxed_slice(),
    })
}
