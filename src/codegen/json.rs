use std::{collections::BTreeMap, error::Error};

use crate::check_query::QueryFn;

use super::CodeGen;

pub struct JsonCodeGen {
    queries: BTreeMap<String, QueryFn>,
}

impl JsonCodeGen {
    pub fn new() -> Self {
        Self {
            queries: BTreeMap::new(),
        }
    }
}

impl CodeGen for JsonCodeGen {
    fn push(&mut self, file_name: &str, query: QueryFn) -> Result<(), Box<dyn Error>> {
        self.queries.insert(file_name.to_string(), query);
        Ok(())
    }

    fn finalize(&self) -> Result<String, Box<dyn Error>> {
        Ok(serde_json::to_string_pretty(&self.queries)?)
    }
}
