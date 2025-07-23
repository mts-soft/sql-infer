use std::collections::HashMap;

use crate::{
    inference::{InformationSchema, SqlType, UseInformationSchema},
    parser::Column,
};

pub struct TextLength;

impl UseInformationSchema for TextLength {
    fn apply(
        &self,
        schemas: &HashMap<Column, InformationSchema>,
        column: &Column,
        item: &mut super::QueryItem,
    ) {
        let schema = schemas.get(column);
        let Some(schema) = schema else {
            return;
        };
        if includes_cast(column) != Some(true) {
            return;
        }
        if let SqlType::Char { length } | SqlType::VarChar { length } = &mut item.sql_type {
            if let Some(character_maximum_length) = schema.character_maximum_length {
                *length = Some(character_maximum_length as u32)
            }
        }
    }
}

pub struct DecimalPrecision;

impl UseInformationSchema for DecimalPrecision {
    fn apply(
        &self,
        schemas: &HashMap<Column, InformationSchema>,
        column: &Column,
        item: &mut super::QueryItem,
    ) {
        let schema = schemas.get(column);
        let Some(schema) = schema else {
            return;
        };
        if includes_cast(column) != Some(true) {
            return;
        }
        if let SqlType::Decimal {
            precision,
            precision_radix,
        } = &mut item.sql_type
        {
            if let Some((numeric_precision, numeric_precision_radix)) =
                schema.numeric_precision.zip(schema.numeric_precision_radix)
            {
                *precision = Some(numeric_precision as u32);
                *precision_radix = Some(numeric_precision_radix as u32);
            };
        }
    }
}

fn includes_cast(column: &Column) -> Option<bool> {
    Some(match column {
        Column::DependsOn { .. } => false,
        Column::Maybe { column } => includes_cast(column)?,
        Column::Either { left, right } => Option::zip(includes_cast(left), includes_cast(right))
            .map(|(left, right)| left || right)?,
        Column::Cast { .. } => true,
        Column::BinaryOp { .. } => return None,
        Column::Unknown { .. } => return None,
        Column::Value { .. } => return None,
    })
}
