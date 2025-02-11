use std::{error::Error, fmt};

use sqlx::{postgres::PgPoolOptions, Executor};
use sqlx::{Column, Either, Statement, TypeInfo};

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

pub struct QueryValue {
    pub name: String,
    pub type_name: String,
}

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

pub async fn check_query(db_url: &str, query: &str) -> Result<QueryTypes, Box<dyn Error>> {
    let prepared_query = prepare_dbapi2(query)?;
    let query = &prepared_query.postgres_query;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await?;
    let prepared = pool.prepare(query).await?;
    let result_types = prepared
        .columns()
        .iter()
        .map(|column| QueryValue {
            name: column.name().to_string(),
            type_name: column.type_info().name().to_owned(),
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
            })
            .collect(),
        Some(Either::Right(_)) => panic!("Postgres connection should never lead here"),
        None => panic!("Parameter types were not provided."),
    };
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
        outs.push(format!(
            "    {}: {} | None",
            query_value.name,
            sql_to_py(&query_value.type_name)?
        ));
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
