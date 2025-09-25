use std::{borrow::Cow, fmt::Display};

use serde::{Deserialize, Serialize};
use sql_infer_core::inference::SqlType;

use crate::schema::DbSchema;

#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LintSetting {
    #[default]
    Allow,
    Warn,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Source {
    Table(String),
    Column { table: String, column: String },
}

#[derive(Debug, Clone)]
pub struct LintError {
    source: Source,
    msg: Cow<'static, str>,
}

impl Display for LintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source {
            Source::Table(table) => write!(f, "[table] {table}: "),
            Source::Column { table, column } => write!(f, "[column] {table}.{column}: "),
        }?;
        write!(f, "{}", self.msg)
    }
}

pub trait Lint {
    fn lint(&self, db: &DbSchema) -> Vec<LintError>;
}
pub struct TimestampWithoutTimezone;

impl Lint for TimestampWithoutTimezone {
    fn lint(&self, db: &DbSchema) -> Vec<LintError> {
        let mut errors = vec![];
        for table in &db.tables {
            for column in &table.columns {
                let SqlType::Timestamp { tz: true } = column.data_type else {
                    continue;
                };
                errors.push(LintError {
                    source: Source::Column {
                        table: table.name.clone(),
                        column: column.name.clone(),
                    },
                    msg: Cow::Borrowed("timestamp has no timezone"),
                });
            }
        }
        errors
    }
}

pub struct TimeWithTimezone;

impl Lint for TimeWithTimezone {
    fn lint(&self, db: &DbSchema) -> Vec<LintError> {
        let mut errors = vec![];
        for table in &db.tables {
            for column in &table.columns {
                let SqlType::Time { tz: false } = column.data_type else {
                    continue;
                };
                errors.push(LintError {
                    source: Source::Column {
                        table: table.name.clone(),
                        column: column.name.clone(),
                    },
                    msg: Cow::Borrowed("time has timezone"),
                });
            }
        }
        errors
    }
}
