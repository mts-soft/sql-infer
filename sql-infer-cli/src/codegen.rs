pub mod json;
pub mod sqlalchemy;
pub mod sqlalchemy_async;

use std::error::Error;

use serde::{Deserialize, Serialize};
use sql_infer_core::inference::QueryItem;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDefinition {
    pub query: String,
    pub inputs: Box<[QueryItem]>,
    pub outputs: Box<[QueryItem]>,
}

pub trait CodeGen {
    fn push(&mut self, name: &str, query: QueryDefinition) -> Result<(), Box<dyn Error>>;

    fn finalize(&self) -> Result<String, Box<dyn Error>>;
}
