use crate::{
    inference::{InformationSchema, Nullability, UseInformationSchema},
    parser::Column,
};

pub struct ColumnNullability;

impl UseInformationSchema for ColumnNullability {
    fn apply(
        &self,
        schema: Option<&InformationSchema>,
        source: &Column,
        column: &mut super::QueryItem,
    ) {
        let Some(schema) = schema else {
            return;
        };
        if column_table_is_nullable(source) == Nullability::True {
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

fn column_table_is_nullable(col: &Column) -> Nullability {
    match col {
        Column::DependsOn { .. } => Nullability::False,
        Column::Maybe { .. } => Nullability::True,
        Column::Either { left, right } => match column_table_is_nullable(left) {
            Nullability::True => Nullability::True,
            Nullability::False => column_table_is_nullable(right),
            Nullability::Unknown => Nullability::Unknown,
        },
        Column::Unknown => Nullability::Unknown,
    }
}
