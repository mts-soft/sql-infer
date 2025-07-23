use std::error::Error;

use crate::inference::{Passes, QueryTypes, UseInformationSchema};

pub mod inference;
pub mod parser;

#[must_use]
pub struct SqlInferBuilder {
    passes: Passes,
}

impl Default for SqlInferBuilder {
    fn default() -> Self {
        Self {
            passes: Passes {
                information_schema: vec![],
            },
        }
    }
}

impl SqlInferBuilder {
    pub fn add_information_schema_pass(
        &mut self,
        pass: impl UseInformationSchema + 'static,
    ) -> &mut Self {
        self.passes.information_schema.push(Box::new(pass));
        self
    }

    pub fn build(self) -> SqlInfer {
        SqlInfer {
            passes: self.passes,
        }
    }
}

pub struct SqlInfer {
    passes: Passes,
}

impl SqlInfer {
    pub async fn infer_types(
        &self,
        pool: &sqlx::Pool<sqlx::Postgres>,
        query: &str,
    ) -> Result<QueryTypes, Box<dyn Error>> {
        inference::check_statement(pool, query, &self.passes).await
    }
}
