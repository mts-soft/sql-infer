use sqlx::{postgres::PgPoolOptions, Executor};
use sqlx::{query, Column, Either, Pool, Postgres, Statement, TypeInfo};
use std::{error::Error, fmt};
use tracing::warn;

use crate::parser::{find_source, to_ast};
use crate::query_converter::prepare_dbapi2;
use crate::utils::to_pascal;

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

#[derive(Debug, Copy, Clone)]
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

fn sql_to_py(sql_type: &str) -> Result<&'static str, Box<dyn Error>> {
    let py_type = match sql_type {
        "BOOL" => "bool",
        "INT2" | "INT4" | "INT" | "INT8" | "SERIAL" | "SMALLINT" | "SMALLSERIAL" => "int",
        "NUMERIC" => "Decimal",
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATE" | "TIME" | "TIMETZ" => "datetime",
        "CHAR" | "VARCHAR" | "TEXT" | "NAME" | "CITEXT" => "str",
        "BIT" | "VARBIT" => "str",
        "JSON" | "JSONB" => todo!(),
        "DOUBLE PRECISION" | "FLOAT4" | "FLOAT8" | "REAL" => "float",
        _ => Err(CheckerError::UnrecognizedType {
            sql_type: sql_type.to_string(),
        })?,
    };
    Ok(py_type)
}

pub async fn get_col_nullability(
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
                let nullability = get_col_nullability(pool, &table.name, &output.name).await?;
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

pub fn query_to_sql_alchemy(
    query_name: &str,
    query: &str,
    query_types: &QueryTypes,
) -> Result<String, Box<dyn Error>> {
    let mut params = vec!["conn: Connection".to_owned()];
    let mut binds = vec![];

    for query_value in &query_types.input {
        let param_name = &query_value.name;
        params.push(format!(
            "{param_name}: {} | None",
            sql_to_py(&query_value.type_name)?
        ));
        binds.push(format!("\"{param_name}\": {param_name}"));
    }
    let mut outs = vec![];

    for query_value in &query_types.output {
        let py_type = sql_to_py(&query_value.type_name)?;
        let output_type = match query_value.nullable {
            Nullability::False => py_type,
            Nullability::Unknown | Nullability::True => &format!("{} | None", py_type),
        };
        outs.push(format!("    {}: {}", query_value.name, output_type,));
    }
    let class_name = to_pascal(&format!("{query_name}_output"));
    let out_types = match outs.is_empty() {
        true => "None",
        false => &format!("DbOutput[{class_name}]"),
    };
    let return_type = match outs.is_empty() {
        true => "",
        false => &format!("@dataclass\nclass {class_name}:\n{}\n", outs.join("\n")),
    };

    let in_types = params.join(", ");
    let function_signature = format!("def {query_name}({in_types}) -> {out_types}:");

    let bind_text = match binds.len() {
        0 => "".to_string(),
        _ => format!("{{{}}}", binds.join(", ")),
    };

    let mut function_content =
        format!("    result = conn.execute(text(\"\"\"{query}\"\"\"), {bind_text})\n");
    if !outs.is_empty() {
        function_content.push_str(&format!(
            "    return DbOutput({class_name}(*row) for row in result) # type: ignore\n"
        ));
    }
    Ok(format!(
        "{return_type}\n\n{function_signature}\n{function_content}"
    ))
}
