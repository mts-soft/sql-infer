pub mod json;
pub mod sqlalchemy;

use std::error::Error;

use crate::{check_query::QueryFn, config::FeatureSet};

pub trait CodeGen {
    fn push(&mut self, file_name: &str, query: QueryFn) -> Result<(), Box<dyn Error>>;

    fn finalize(&self, features: &FeatureSet) -> Result<String, Box<dyn Error>>;
}
