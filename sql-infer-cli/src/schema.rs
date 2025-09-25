pub mod lint;

use std::{cmp, fmt::Display};

use serde::{Deserialize, Serialize};
use sql_infer_core::inference::SqlType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: SqlType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbSchema {
    pub tables: Vec<TableSchema>,
}

impl Display for DbSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for table in &self.tables {
            writeln!(f, "{}", table.name)?;
            let column_names = table
                .columns
                .iter()
                .map(|col| match col.nullable {
                    true => format!("{}?", col.name),
                    false => col.name.clone(),
                })
                .collect::<Vec<_>>();
            let type_names = table
                .columns
                .iter()
                .map(|col| col.data_type.to_string())
                .collect::<Vec<_>>();
            let lengths = column_names
                .iter()
                .zip(&type_names)
                .map(|(left, right)| cmp::max(left.len(), right.len()))
                .collect::<Vec<_>>();
            let column_names = column_names
                .iter()
                .zip(lengths.clone())
                .map(|(name, len)| format!("{name}{}", " ".repeat(len - name.len())))
                .collect::<Vec<_>>()
                .join("  |  ");
            let type_names = type_names
                .into_iter()
                .zip(lengths)
                .map(|(name, len)| format!("{name}{}", " ".repeat(len - name.len())))
                .collect::<Vec<_>>()
                .join("  |  ");
            writeln!(f, "{column_names}")?;
            writeln!(f, "{type_names}")?;
            writeln!(f)?;
        }
        Ok(())
    }
}
