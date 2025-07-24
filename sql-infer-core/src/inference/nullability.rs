use std::collections::HashMap;

use crate::{
    inference::{InformationSchema, Nullability, UseInformationSchema},
    parser::{Column, ValueType},
};

pub struct ColumnNullability;

impl UseInformationSchema for ColumnNullability {
    fn apply(
        &self,
        schemas: &HashMap<Column, InformationSchema>,
        source: &Column,
        column: &mut super::QueryItem,
    ) {
        column.nullable = column_is_nullable(source, schemas);
    }
}

fn column_is_nullable(col: &Column, schemas: &HashMap<Column, InformationSchema>) -> Nullability {
    match col {
        Column::DependsOn { .. } => {
            schemas
                .get(col)
                .map_or(Nullability::Unknown, |schema| match schema.is_nullable {
                    Some(true) => Nullability::True,
                    Some(false) => Nullability::False,
                    None => Nullability::Unknown,
                })
        }
        Column::Maybe { .. } => Nullability::True,
        Column::Either { left, right } => match column_is_nullable(left, schemas) {
            Nullability::True => Nullability::True,
            Nullability::False => column_is_nullable(right, schemas),
            Nullability::Unknown => Nullability::Unknown,
        },
        Column::Unknown { .. } => Nullability::Unknown,
        Column::Cast { source, .. } => column_is_nullable(source, schemas),
        Column::BinaryOp { op, left, right } => {
            if op.not_null() == Some(true) {
                return Nullability::False;
            }
            match column_is_nullable(left, schemas) {
                Nullability::True => Nullability::True,
                Nullability::False => column_is_nullable(right, schemas),
                Nullability::Unknown => Nullability::Unknown,
            }
        }
        Column::Value(value_type) => match value_type {
            ValueType::Null => Nullability::True,
            _ => Nullability::False,
        },
    }
}
