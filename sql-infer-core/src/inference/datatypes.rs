use crate::inference::{SqlType, UseInformationSchema};

pub struct TextLength;

impl UseInformationSchema for TextLength {
    fn apply(
        &self,
        schema: &super::InformationSchema,
        _: &mut super::DbTable,
        column: &mut super::QueryItem,
    ) {
        if let SqlType::Char { length } | SqlType::VarChar { length } = &mut column.sql_type {
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
        schema: &super::InformationSchema,
        _: &mut super::DbTable,
        column: &mut super::QueryItem,
    ) {
        if let SqlType::Decimal {
            precision,
            precision_radix,
        } = &mut column.sql_type
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
