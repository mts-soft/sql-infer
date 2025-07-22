use crate::inference::{Nullability, UseInformationSchema};

pub struct ColumnNullability;

impl UseInformationSchema for ColumnNullability {
    fn apply(
        &self,
        schema: &super::InformationSchema,
        table: &mut super::DbTable,
        column: &mut super::QueryItem,
    ) {
        if table.nullable {
            column.nullable = Nullability::True;
            return;
        }
        match schema.is_nullable {
            Some(true) => column.nullable = Nullability::True,
            Some(false) => column.nullable = Nullability::False,
            None => column.nullable = Nullability::Unknown,
        }
    }
}
