use std::{collections::BTreeMap, error::Error};

use crate::{
    check_query::{Nullability, QueryFn, QueryItem, SqlType},
    config::FeatureSet,
};

use super::CodeGen;

fn to_pascal(mixed_case_name: &str) -> String {
    let mut words = vec![];
    let mut curr = String::new();
    for character in mixed_case_name.chars() {
        let is_snake = character == '_';
        if character.is_uppercase() || is_snake {
            words.push(curr.clone());
            curr.clear();
        }
        if is_snake {
            continue;
        }
        if curr.is_empty() {
            curr.push(character.to_ascii_uppercase());
        } else {
            curr.push(character.to_ascii_lowercase());
        }
    }
    words.push(curr);
    words.join("")
}

fn to_py_input_type(item: &QueryItem) -> String {
    let py_type = match item.sql_type {
        SqlType::Bool => "bool",
        SqlType::Int2
        | SqlType::Int4
        | SqlType::Int8
        | SqlType::SmallSerial
        | SqlType::Serial
        | SqlType::BigSerial => "int",
        SqlType::Decimal { .. } => "Decimal",
        SqlType::Timestamp { .. } => "datetime",
        SqlType::Date => "date",
        SqlType::Time { .. } => "time",
        SqlType::Char { .. }
        | SqlType::VarChar { .. }
        | SqlType::Text
        | SqlType::Json
        | SqlType::Jsonb => "str",
        SqlType::Float4 | SqlType::Float8 => "float",
        SqlType::Interval => "timedelta",
        SqlType::Bit { .. } | SqlType::VarBit { .. } => "str",
    }
    .to_owned();
    match item.nullable {
        Nullability::True | Nullability::Unknown => format!("{} | None", py_type),
        Nullability::False => py_type,
    }
}

fn to_py_output_type(item: &QueryItem) -> String {
    let py_type = match item.sql_type {
        SqlType::Json | SqlType::Jsonb => "Json",
        _ => return to_py_input_type(item),
    }
    .to_owned();
    match item.nullable {
        Nullability::True | Nullability::Unknown => format!("{} | None", py_type),
        Nullability::False => py_type,
    }
}

fn query_to_sql_alchemy(fn_name: &str, query_fn: &QueryFn) -> Result<String, Box<dyn Error>> {
    let mut params = vec!["conn: Connection".to_owned()];
    let mut binds = vec![];

    for query_value in &query_fn.inputs {
        let param_name = &query_value.name;
        params.push(format!("{}: {}", param_name, to_py_input_type(query_value)));
        binds.push(format!("\"{param_name}\": {param_name}"));
    }
    let mut outs = vec![];

    for query_value in &query_fn.outputs {
        let py_type = to_py_output_type(query_value);
        outs.push(format!("    {}: {}", query_value.name, py_type));
    }
    let class_name = to_pascal(&format!("{fn_name}_output"));
    let out_types = match outs.is_empty() {
        true => "None",
        false => &format!("DbOutput[{class_name}]"),
    };
    let return_type = match outs.is_empty() {
        true => "",
        false => &format!("@dataclass\nclass {class_name}:\n{}\n", outs.join("\n")),
    };

    let in_types = params.join(", ");
    let function_signature = format!("def {fn_name}({in_types}) -> {out_types}:");

    let bind_text = match binds.len() {
        0 => "".to_string(),
        _ => format!("{{{}}}", binds.join(", ")),
    };

    let mut function_content = format!(
        "    result = conn.execute(text(\"\"\"{}\"\"\"), {})\n",
        query_fn.query, bind_text
    );
    if !outs.is_empty() {
        function_content.push_str(&format!(
            "    return DbOutput({class_name}(*row) for row in result) # type: ignore\n"
        ));
    }
    Ok(format!(
        "{return_type}\n\n{function_signature}\n{function_content}"
    ))
}

pub struct SqlAlchemyCodeGen {
    queries: BTreeMap<String, QueryFn>,
}

impl SqlAlchemyCodeGen {
    pub fn new() -> Self {
        SqlAlchemyCodeGen {
            queries: BTreeMap::new(),
        }
    }
}

impl CodeGen for SqlAlchemyCodeGen {
    fn push(&mut self, file_name: &str, query: QueryFn) -> Result<(), Box<dyn Error>> {
        self.queries.insert(file_name.to_string(), query);
        Ok(())
    }

    fn finalize(&self, _: &FeatureSet) -> Result<String, Box<dyn Error>> {
        let mut code = include_str!("./sqlalchemy/template.txt").to_string();
        for (file_name, query) in &self.queries {
            let func = query_to_sql_alchemy(file_name, query)?;
            code.push_str(&func);
            code.push('\n');
        }
        Ok(code)
    }
}
