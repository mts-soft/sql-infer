pub mod json;
pub mod sqlalchemy;

use std::error::Error;

use crate::check_query::QueryFn;

pub trait CodeGen {
    fn push(&mut self, file_name: &str, query: QueryFn) -> Result<(), Box<dyn Error>>;

    fn finalize(&self) -> Result<String, Box<dyn Error>>;
}
