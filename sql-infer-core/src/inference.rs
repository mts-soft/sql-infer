pub mod datatypes;
pub mod nullability;

use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgTypeInfo, PgTypeKind};
use sqlx::{Either, Pool, Postgres, Statement, TypeInfo};
use sqlx::{Executor, query_as};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::{error::Error, fmt};

use crate::parser::{Column, find_fields, to_ast};
use tracing::warn;

pub trait UseInformationSchema {
    fn apply(
        &self,
        schemas: &HashMap<Column, InformationSchema>,
        source: &Column,
        column: &mut QueryItem,
    );
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    // Enum
    Enum {
        name: String,
        tags: Arc<[String]>,
    },
    // Unknown types
    Unknown,
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
                    "with time zone"
                } else {
                    "without time zone"
                }
            ),
            SqlType::Date => write!(f, "date"),
            SqlType::Time { tz } => write!(
                f,
                "time {}",
                if *tz {
                    "with time zone"
                } else {
                    "without time zone"
                }
            ),
            SqlType::Interval => write!(f, "interval"),
            SqlType::Char { length } => {
                if let Some(length) = length {
                    write!(f, "char({length})")
                } else {
                    write!(f, "char")
                }
            }
            SqlType::VarChar { length } => {
                if let Some(length) = length {
                    write!(f, "varchar({length})")
                } else {
                    write!(f, "varchar")
                }
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
            SqlType::Unknown => write!(f, "unknown"),
            SqlType::Enum { name, tags } => write!(f, "{name}: {}", tags.join(", ")),
        }
    }
}

impl SqlType {
    pub fn is_numeric(&self) -> bool {
        match self {
            SqlType::Bool => false,
            SqlType::Int2
            | SqlType::Int4
            | SqlType::Int8
            | SqlType::SmallSerial
            | SqlType::Serial
            | SqlType::BigSerial
            | SqlType::Decimal { .. }
            | SqlType::Float4
            | SqlType::Float8 => true,
            _ => false,
        }
    }

    pub fn is_text(&self) -> bool {
        matches!(
            self,
            SqlType::Char { .. } | SqlType::VarChar { .. } | SqlType::Text
        )
    }

    fn numeric_rank(&self) -> Option<u8> {
        // https://www.postgresql.org/docs/current/functions-math.html
        Some(match self {
            SqlType::Int2 | SqlType::SmallSerial => 0,
            SqlType::Int4 | SqlType::Serial => 1,
            SqlType::Int8 | SqlType::BigSerial => 2,
            SqlType::Decimal { .. } => 3,
            SqlType::Float4 => 4,
            SqlType::Float8 => 5,
            _ => return None,
        })
    }

    pub fn numeric_compare(&self, other: &Self) -> Option<Ordering> {
        self.numeric_rank()
            .zip(other.numeric_rank())
            .map(|(left, right)| left.cmp(&right))
    }

    fn from_pg_type_info(type_info: &PgTypeInfo) -> Result<Self, Box<dyn Error>> {
        Ok(match type_info.kind() {
            PgTypeKind::Enum(items) => SqlType::Enum {
                name: type_info.name().to_string(),
                tags: items.clone(),
            },
            _ => SqlType::from_str(type_info.name())?,
        })
    }

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
            "TIME" => Self::Time { tz: false },
            "TIMETZ" => Self::Time { tz: true },
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
            _ => Self::Unknown,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InformationSchema {
    pub is_nullable: Option<bool>,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub numeric_precision_radix: Option<i32>,
    pub numeric_scale: Option<i32>,
    pub column_default: Option<String>,
}

async fn get_information_schema(
    pool: &Pool<Postgres>,
    table: &str,
    column: &str,
) -> Result<Option<InformationSchema>, Box<dyn Error>> {
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
        table,
        column,
    );
    Ok(query.fetch_optional(pool).await?)
}

pub async fn get_all_info_schema(
    pool: &Pool<Postgres>,
    source: &Column,
    map: &mut HashMap<Column, InformationSchema>,
) -> Result<Option<InformationSchema>, Box<dyn Error>> {
    let schema = match source {
        Column::DependsOn { table, column } => get_information_schema(pool, table, column).await?,
        Column::Maybe { column } => Box::pin(get_all_info_schema(pool, column, map)).await?,
        Column::Either { left, right } => {
            let future = Box::pin(async {
                let left = get_all_info_schema(pool, left, map).await?;
                let right = get_all_info_schema(pool, right, map).await?;
                Ok::<_, Box<dyn Error>>((left, right))
            });
            let (left, right) = future.await?;
            match (left, right) {
                (None, None) => None,
                (None, Some(right)) => Some(right),
                (Some(left), None) => Some(left),
                (Some(_), Some(_)) => None,
            }
        }
        Column::Unknown { .. } => None,
        Column::Cast { source, .. } => Box::pin(get_all_info_schema(pool, source, map)).await?,
        Column::BinaryOp { left, right, .. } => {
            Box::pin(get_all_info_schema(pool, left, map)).await?;
            Box::pin(get_all_info_schema(pool, right, map)).await?;
            None
        }
        Column::Value(_) => None,
    };
    if let Some(schema) = &schema {
        map.insert(source.clone(), schema.clone());
    }
    Ok(schema)
}

pub async fn get_column_information_schema(
    pool: &Pool<Postgres>,
    source: &Column,
) -> Result<(Column, Option<InformationSchema>), Box<dyn Error>> {
    match source {
        Column::DependsOn { table, column } => Ok((
            source.clone(),
            get_information_schema(pool, table, column).await?,
        )),
        Column::Maybe { column } => {
            let (column, schema) = Box::pin(get_column_information_schema(pool, column)).await?;
            Ok((column.maybe(), schema))
        }
        Column::Either { left, right } => {
            let future = Box::pin(async {
                let left = get_column_information_schema(pool, left).await?;
                let right = get_column_information_schema(pool, right).await?;
                Ok::<_, Box<dyn Error>>((left, right))
            });
            let ((left_col, left), (right_col, right)) = future.await?;
            Ok(match (left, right) {
                (None, None) => (source.clone(), None),
                (None, Some(right)) => (right_col, Some(right)),
                (Some(left), None) => (left_col, Some(left)),
                (Some(_), Some(_)) => (source.clone(), None),
            })
        }
        Column::Unknown { .. } => Ok((source.clone(), None)),
        Column::Cast { source, data_type } => {
            let (column, schema) = Box::pin(get_column_information_schema(pool, source)).await?;
            Ok((column.cast(data_type.clone()), schema))
        }
        Column::BinaryOp { .. } => Ok((source.clone(), None)),
        Column::Value(_) => Ok((source.clone(), None)),
    }
}

pub(crate) async fn update_with_info(
    pool: &Pool<Postgres>,
    source: &Column,
    item: &mut QueryItem,
    passes: &Passes,
) -> Result<(), Box<dyn Error>> {
    let mut map = HashMap::new();
    get_all_info_schema(pool, source, &mut map).await?;
    for pass in &passes.information_schema {
        pass.apply(&map, source, item);
    }
    Ok(())
}

pub(crate) async fn apply_passes(
    pool: &Pool<Postgres>,
    query: &str,
    output_types: &mut [QueryItem],
    passes: &Passes,
) -> Result<(), Box<dyn Error>> {
    let statement = to_ast(query)?;
    let statement = statement.first().ok_or("Empty query")?;
    let mut errors: Vec<String> = vec![];

    let fields = find_fields(statement)?;
    for output in output_types.iter_mut() {
        match fields.get(&output.name) {
            Some(column) => {
                update_with_info(pool, column, output, passes).await?;
            }
            None => errors.push(format!("not provided with info for {}", output.name)),
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
    use sqlx::Column;
    let prepared = pool.prepare(query).await?;
    let mut result_types = Vec::with_capacity(prepared.columns().len());
    for column in prepared.columns() {
        result_types.push(QueryItem {
            name: column.name().to_string(),
            sql_type: SqlType::from_pg_type_info(column.type_info())?,
            nullable: Nullability::Unknown,
        });
    }
    let mut input_types = vec![];
    match prepared.parameters() {
        Some(Either::Left(parameters)) => {
            for (param, name) in parameters.iter().zip(parameters.iter()) {
                input_types.push(QueryItem {
                    name: name.to_string(),
                    sql_type: SqlType::from_pg_type_info(param)?,
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
