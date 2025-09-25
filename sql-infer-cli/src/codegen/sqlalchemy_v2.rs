use std::{collections::BTreeMap, error::Error};

use serde::{Deserialize, Serialize};
use sql_infer_core::inference::{Nullability, QueryItem, SqlType};

use crate::codegen::QueryDefinition;

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
    let py_type = match &item.sql_type {
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
        SqlType::Enum { tags, .. } => {
            return format!(
                "Literal[{}]",
                tags.iter()
                    .map(|tag| format!("{tag:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        SqlType::Unknown => "Any",
    }
    .to_owned();
    match item.nullable {
        Nullability::True | Nullability::Unknown => format!("{py_type} | None"),
        Nullability::False => py_type,
    }
}

fn to_pydantic_input_type(item: &QueryItem) -> String {
    let py_type = match &item.sql_type {
        SqlType::Bool => "bool",
        SqlType::Int2
        | SqlType::Int4
        | SqlType::Int8
        | SqlType::SmallSerial
        | SqlType::Serial
        | SqlType::BigSerial => "int",
        SqlType::Decimal { .. } => "Decimal",
        SqlType::Timestamp { tz: false } => "NaiveDatetime",
        SqlType::Timestamp { tz: true } => "AwareDatetime",
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
        SqlType::Enum { tags, .. } => {
            return format!(
                "Literal[{}]",
                tags.iter()
                    .map(|tag| format!("{tag:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        SqlType::Unknown => "Any",
    }
    .to_owned();
    match item.nullable {
        Nullability::True | Nullability::Unknown => format!("{py_type} | None"),
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
        Nullability::True | Nullability::Unknown => format!("{py_type} | None"),
        Nullability::False => py_type,
    }
}

fn to_pydantic_output_type(item: &QueryItem) -> String {
    let py_type = match item.sql_type {
        SqlType::Json | SqlType::Jsonb => "Json",
        _ => return to_py_input_type(item),
    }
    .to_owned();
    match item.nullable {
        Nullability::True | Nullability::Unknown => format!("{py_type} | None"),
        Nullability::False => py_type,
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ArgumentMode {
    #[default]
    Positional,
    Keyword,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TypeGen {
    #[default]
    Python,
    Pydantic,
}

#[derive(Default)]
pub struct SqlAlchemyV2CodeGen {
    queries: BTreeMap<String, QueryDefinition>,
    r#async: bool,
    argument_mode: ArgumentMode,
    type_gen: TypeGen,
}

impl SqlAlchemyV2CodeGen {
    pub fn new(r#async: bool, argument_mode: ArgumentMode, type_gen: TypeGen) -> Self {
        Self {
            queries: Default::default(),
            r#async,
            argument_mode,
            type_gen,
        }
    }

    fn conn_param(&self) -> &str {
        match self.r#async {
            false => "conn: Connection",
            true => "conn: AsyncConnection",
        }
    }

    fn to_input_type(&self, item: &QueryItem) -> String {
        match self.type_gen {
            TypeGen::Python => to_py_input_type(item),
            TypeGen::Pydantic => to_pydantic_input_type(item),
        }
    }

    fn to_output_type(&self, item: &QueryItem) -> String {
        match self.type_gen {
            TypeGen::Python => to_py_output_type(item),
            TypeGen::Pydantic => to_pydantic_output_type(item),
        }
    }

    fn query_to_sql_alchemy(
        &self,
        fn_name: &str,
        query_fn: &QueryDefinition,
    ) -> Result<String, Box<dyn Error>> {
        let mut params = vec![self.conn_param().to_string()];
        if !query_fn.inputs.is_empty() && self.argument_mode == ArgumentMode::Keyword {
            params.push("*".to_string());
        }
        let mut binds = vec![];

        for query_value in &query_fn.inputs {
            let param_name = &query_value.name;
            params.push(format!(
                "{}: {}",
                param_name,
                self.to_input_type(query_value)
            ));
            binds.push(format!("\"{param_name}\": {param_name}"));
        }
        let mut outs = vec![];

        for query_value in &query_fn.outputs {
            let py_type = self.to_output_type(query_value);
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
        let function_signature = format!("async def {fn_name}({in_types}) -> {out_types}:");

        let bind_text = match binds.len() {
            0 => "".to_string(),
            _ => format!("{{{}}}", binds.join(", ")),
        };

        let mut function_content = format!(
            "    result = await conn.execute(text(\"\"\"{}\"\"\"), {})\n",
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
}

impl CodeGen for SqlAlchemyV2CodeGen {
    fn push(&mut self, file_name: &str, query: QueryDefinition) -> Result<(), Box<dyn Error>> {
        self.queries.insert(file_name.to_string(), query);
        Ok(())
    }

    fn finalize(&self) -> Result<String, Box<dyn Error>> {
        let mut code = match self.r#async {
            true => include_str!("./sqlalchemy_async/template.txt").to_string(),
            false => include_str!("./sqlalchemy/template.txt").to_string(),
        };
        if self.type_gen == TypeGen::Pydantic {
            code += "\nfrom pydantic import AwareDatetime, NaiveDatetime"
        }
        for (file_name, query) in &self.queries {
            let func = self.query_to_sql_alchemy(file_name, query)?;
            code.push_str(&func);
            code.push('\n');
        }
        Ok(code)
    }
}
