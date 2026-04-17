use std::{borrow::Cow, collections::BTreeMap, error::Error, fmt::Display};

use serde::{Deserialize, Serialize};
use sql_infer_core::inference::{Nullability, QueryItem, SqlType};

use crate::codegen::{QueryDefinition, py_utils::escape_string};

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

trait TypeBounds: Display {
    fn bounds(&mut self, r#type: &str) -> String;
}

#[derive(Debug, Copy, Clone)]
pub struct PyTypeVar(usize);

impl Display for PyTypeVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "T{}", self.0)
    }
}
struct ParamTypeBounds {
    bounds: Vec<String>,
}

impl Display for ParamTypeBounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.bounds.is_empty() {
            return Ok(());
        }
        write!(
            f,
            "[{}]",
            self.bounds
                .iter()
                .enumerate()
                .map(|(idx, ty)| format!("{}: {ty}", PyTypeVar(idx)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl TypeBounds for ParamTypeBounds {
    fn bounds(&mut self, r#type: &str) -> String {
        let idx = self.bounds.len();
        self.bounds.push(r#type.to_string());
        PyTypeVar(idx).to_string()
    }
}

struct NoBounds;

impl Display for NoBounds {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl TypeBounds for NoBounds {
    fn bounds(&mut self, r#type: &str) -> String {
        r#type.to_string()
    }
}

fn to_py_input_type(
    sql_type: &SqlType,
    nullable: Nullability,
    bounds: &mut dyn TypeBounds,
) -> String {
    let py_type: Cow<'_, str> = match sql_type {
        SqlType::Bool => Cow::Borrowed("bool"),
        SqlType::Int2
        | SqlType::Int4
        | SqlType::Int8
        | SqlType::SmallSerial
        | SqlType::Serial
        | SqlType::BigSerial => Cow::Borrowed("int"),
        SqlType::Decimal { .. } => Cow::Borrowed("Decimal"),
        SqlType::Timestamp { .. } => Cow::Borrowed("datetime"),
        SqlType::Date => Cow::Borrowed("date"),
        SqlType::Time { .. } => Cow::Borrowed("time"),
        SqlType::Char { .. }
        | SqlType::VarChar { .. }
        | SqlType::Text
        | SqlType::Json
        | SqlType::Jsonb => Cow::Borrowed("str"),
        SqlType::Float4 | SqlType::Float8 => Cow::Borrowed("float"),
        SqlType::Interval => Cow::Borrowed("timedelta"),
        SqlType::Bit { .. } | SqlType::VarBit { .. } => Cow::Borrowed("str"),
        SqlType::Enum { tags, .. } => Cow::Owned(format!(
            "Literal[{}]",
            tags.iter()
                .map(|tag| format!("{:?}", escape_string(tag)))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        SqlType::Unknown => Cow::Borrowed("Any"),
        SqlType::Array(inner_type) => {
            let inner = to_py_input_type(inner_type, Nullability::True, bounds);
            let var = bounds.bounds(&inner);
            Cow::Owned(format!("list[{var}]"))
        }
    };
    match nullable {
        Nullability::True | Nullability::Unknown => format!("{py_type} | None"),
        Nullability::False => py_type.to_string(),
    }
}

fn to_pydantic_input_type(
    sql_type: &SqlType,
    nullable: Nullability,
    bounds: &mut dyn TypeBounds,
) -> String {
    let py_type: Cow<'_, str> = match &sql_type {
        SqlType::Bool => Cow::Borrowed("bool"),
        SqlType::Int2
        | SqlType::Int4
        | SqlType::Int8
        | SqlType::SmallSerial
        | SqlType::Serial
        | SqlType::BigSerial => Cow::Borrowed("int"),
        SqlType::Decimal { .. } => Cow::Borrowed("Decimal"),
        SqlType::Timestamp { tz: false } => Cow::Borrowed("NaiveDatetime"),
        SqlType::Timestamp { tz: true } => Cow::Borrowed("AwareDatetime"),
        SqlType::Date => Cow::Borrowed("date"),
        SqlType::Time { .. } => Cow::Borrowed("time"),
        SqlType::Char { .. }
        | SqlType::VarChar { .. }
        | SqlType::Text
        | SqlType::Json
        | SqlType::Jsonb => Cow::Borrowed("str"),
        SqlType::Float4 | SqlType::Float8 => Cow::Borrowed("float"),
        SqlType::Interval => Cow::Borrowed("timedelta"),
        SqlType::Bit { .. } | SqlType::VarBit { .. } => Cow::Borrowed("str"),
        SqlType::Enum { tags, .. } => Cow::Owned(format!(
            "Literal[{}]",
            tags.iter()
                .map(|tag| format!("{:?}", escape_string(tag)))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        SqlType::Unknown => Cow::Borrowed("Any"),
        SqlType::Array(inner_type) => {
            let inner = to_pydantic_input_type(inner_type, Nullability::True, bounds);
            let var = bounds.bounds(&inner);
            Cow::Owned(format!("list[{var}]"))
        }
    };
    match nullable {
        Nullability::True | Nullability::Unknown => format!("{py_type} | None"),
        Nullability::False => py_type.to_string(),
    }
}

fn to_py_output_type(item: &QueryItem) -> String {
    let py_type = match item.sql_type {
        SqlType::Json | SqlType::Jsonb => "Json",
        _ => {
            return to_py_input_type(&item.sql_type, item.nullable, &mut NoBounds);
        }
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
        _ => {
            return to_pydantic_input_type(&item.sql_type, item.nullable, &mut NoBounds);
        }
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
    generic_param_types: bool,
}

impl SqlAlchemyV2CodeGen {
    pub fn new(
        r#async: bool,
        argument_mode: ArgumentMode,
        type_gen: TypeGen,
        generic_param_types: bool,
    ) -> Self {
        Self {
            queries: Default::default(),
            r#async,
            argument_mode,
            type_gen,
            generic_param_types,
        }
    }

    fn conn_param(&self) -> &str {
        match self.r#async {
            false => "conn: Connection",
            true => "conn: AsyncConnection",
        }
    }

    fn to_input_type(&self, item: &QueryItem, bounds: &mut dyn TypeBounds) -> String {
        match self.type_gen {
            TypeGen::Python => to_py_input_type(&item.sql_type, item.nullable, bounds),
            TypeGen::Pydantic => to_pydantic_input_type(&item.sql_type, item.nullable, bounds),
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
        is_async: bool,
    ) -> Result<String, Box<dyn Error>> {
        let mut params = vec![self.conn_param().to_string()];
        if !query_fn.inputs.is_empty() && self.argument_mode == ArgumentMode::Keyword {
            params.push("*".to_string());
        }
        let mut binds = vec![];

        let bounds: &mut dyn TypeBounds = if self.generic_param_types {
            &mut ParamTypeBounds { bounds: vec![] }
        } else {
            &mut NoBounds {}
        };
        for query_value in &query_fn.inputs {
            let param_name = &query_value.name;
            params.push(format!(
                "{}: {}",
                param_name,
                self.to_input_type(query_value, &mut *bounds)
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
        let function_signature = match is_async {
            true => format!("async def {fn_name}{bounds}({in_types}) -> {out_types}:"),
            false => format!("def {fn_name}{bounds}({in_types}) -> {out_types}:"),
        };

        let bind_text = match binds.len() {
            0 => "".to_string(),
            _ => format!("{{{}}}", binds.join(", ")),
        };

        let mut function_content = match is_async {
            true => format!(
                "    result = await conn.execute(text(\"\"\"{}\"\"\"), {})\n",
                query_fn.query, bind_text
            ),
            false => format!(
                "    result = conn.execute(text(\"\"\"{}\"\"\"), {})\n",
                query_fn.query, bind_text
            ),
        };
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
            code += "\nfrom pydantic import AwareDatetime, NaiveDatetime\n"
        }
        for (file_name, query) in &self.queries {
            let func = self.query_to_sql_alchemy(file_name, query, self.r#async)?;
            code.push_str(&func);
            code.push('\n');
        }
        Ok(code)
    }
}
