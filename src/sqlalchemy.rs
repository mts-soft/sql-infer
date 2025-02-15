use std::error::Error;

use crate::check_query::{QueryFn, QueryItem, SqlType};

pub fn to_pascal(mixed_case_name: &str) -> String {
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

pub fn to_py_type(item: &QueryItem) -> String {
    let py_type = match item.sql_type {
        SqlType::Bool => "bool",
        SqlType::Int2
        | SqlType::Int4
        | SqlType::Int8
        | SqlType::SmallSerial
        | SqlType::Serial
        | SqlType::BigSerial => "int",
        SqlType::Decimal => "Decimal",
        SqlType::Timestamp { .. } => "datetime",
        SqlType::Date => "date",
        SqlType::Time { .. } => "time",
        SqlType::Char { .. }
        | SqlType::VarChar { .. }
        | SqlType::Text
        | SqlType::Json
        | SqlType::Jsonb => "str",
        SqlType::Float4 | SqlType::Float8 => "float",
    }
    .to_string();
    match item.nullable {
        true => format!("{} | None", py_type),
        false => py_type,
    }
}

pub fn query_to_sql_alchemy(fn_name: &str, query_fn: &QueryFn) -> Result<String, Box<dyn Error>> {
    let mut params = vec!["conn: Connection".to_owned()];
    let mut binds = vec![];

    for query_value in &query_fn.inputs {
        let param_name = &query_value.name;
        params.push(format!("{}: {}", param_name, to_py_type(query_value)));
        binds.push(format!("\"{param_name}\": {param_name}"));
    }
    let mut outs = vec![];

    for query_value in &query_fn.outputs {
        let py_type = to_py_type(query_value);
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
