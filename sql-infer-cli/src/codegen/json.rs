use std::{collections::BTreeMap, error::Error};

use crate::codegen::QueryDefinition;

use super::CodeGen;

#[derive(Default)]
pub struct JsonCodeGen {
    queries: BTreeMap<String, QueryDefinition>,
}

impl CodeGen for JsonCodeGen {
    fn push(&mut self, file_name: &str, query: QueryDefinition) -> Result<(), Box<dyn Error>> {
        self.queries.insert(file_name.to_string(), query);
        Ok(())
    }

    fn finalize(&self) -> Result<String, Box<dyn Error>> {
        Ok(serde_json::to_string_pretty(&self.queries)?)
    }
}
