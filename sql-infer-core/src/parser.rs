use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;

use sqlparser::ast::{
    Expr, FromTable, JoinOperator, SelectItem, SetExpr, Statement, TableFactor, TableObject,
    TableWithJoins,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone)]
pub enum ParserError {
    UnsupportedStatement { statement: String },
    UnsupportedQueryElement { name: String },
    UnsupportedTableType { msg: String },
}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserError::UnsupportedStatement { statement } => {
                write!(f, "Unrecognized statement: {statement}")
            }
            ParserError::UnsupportedQueryElement { name } => {
                write!(f, "{name} is not supported for queries")
            }
            ParserError::UnsupportedTableType { msg } => {
                write!(f, "Unsupported table type: {msg}")
            }
        }
    }
}

impl Error for ParserError {}

#[derive(Debug, Clone)]
pub enum Table {
    Db {
        name: String,
    },
    Alias {
        name: String,
        source: Arc<Table>,
    },
    Join {
        left: (bool, Arc<Table>),
        right: (bool, Arc<Table>),
    },
}

#[derive(Debug, Clone)]
pub enum Column {
    DependsOn {
        table: String,
        column: String,
    },
    Maybe {
        column: Arc<Column>,
    },
    Either {
        left: Arc<Column>,
        right: Arc<Column>,
    },
    Unknown,
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::DependsOn {
                    table: l_table,
                    column: l_column,
                },
                Self::DependsOn {
                    table: r_table,
                    column: r_column,
                },
            ) => l_table == r_table && l_column == r_column,
            (Self::Maybe { column: l_column }, Self::Maybe { column: r_column }) => {
                l_column == r_column
            }
            (
                Self::Either {
                    left: l_left,
                    right: l_right,
                },
                Self::Either {
                    left: r_left,
                    right: r_right,
                },
            ) => {
                (l_left == r_left && l_right == r_right) || (l_left == r_right && l_right == r_left)
            }
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl Column {
    pub fn depends_on(table: impl Into<String>, column: impl Into<String>) -> Column {
        Self::DependsOn {
            table: table.into(),
            column: column.into(),
        }
    }

    pub fn either(left: Column, right: Column) -> Self {
        Self::Either {
            left: left.into(),
            right: right.into(),
        }
    }

    pub fn maybe(self) -> Self {
        Self::Maybe {
            column: self.into(),
        }
    }

    pub fn unknown(&self) -> bool {
        match self {
            Column::DependsOn { .. } => false,
            Column::Maybe { column } => column.unknown(),
            Column::Either { left, right } => left.unknown() && right.unknown(),
            Column::Unknown => true,
        }
    }
}

impl Table {
    pub fn new(name: impl ToString) -> Arc<Self> {
        Self::Db {
            name: name.to_string(),
        }
        .into()
    }

    pub fn alias(name: impl ToString, source: Arc<Table>) -> Arc<Self> {
        Self::Alias {
            name: name.to_string(),
            source,
        }
        .into()
    }

    pub fn join(left: (bool, Arc<Table>), right: (bool, Arc<Table>)) -> Arc<Self> {
        Self::Join { left, right }.into()
    }

    pub fn find_table_column(&self, table: &str, ident: &str) -> Option<Column> {
        match self {
            Table::Db { name } => match name == table {
                true => Some(Column::depends_on(table, ident)),
                false => None,
            },
            Table::Alias { name, source } => match name == table {
                true => Some(source.find_column(ident)),
                false => None,
            },
            Table::Join {
                left: (left_null, left),
                right: (right_null, right),
            } => {
                let left = left.find_table_column(table, ident);
                let right = right.find_table_column(table, ident);
                let left = match left_null {
                    true => left.map(Column::maybe),
                    false => left,
                };
                let right = match right_null {
                    true => right.map(Column::maybe),
                    false => right,
                };
                match (left, right) {
                    (None, None) => None,
                    (None, Some(right)) => Some(right),
                    (Some(left), None) => Some(left),
                    (Some(left), Some(right)) => Some(Column::either(left, right)),
                }
            }
        }
    }

    pub fn find_column(&self, ident: &str) -> Column {
        match self {
            Table::Db { name } => Column::depends_on(name, ident),
            Table::Alias { source, .. } => source.find_column(ident),
            Table::Join {
                left: (left_null, left),
                right: (right_null, right),
            } => {
                let left = left.find_column(ident);
                let right = right.find_column(ident);
                let left = match left_null {
                    true => left.maybe(),
                    false => left,
                };
                let right = match right_null {
                    true => right.maybe(),
                    false => right,
                };
                Column::either(left, right)
            }
        }
    }
}

fn relation_tables(table_factor: &TableFactor) -> Result<Arc<Table>, ParserError> {
    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let table = Table::new(name);
            Ok(match alias {
                Some(alias) => Table::alias(alias, table),
                None => table,
            })
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let table = get_join(table_with_joins)?;
            Ok(match alias {
                Some(alias) => Table::alias(alias, table),
                None => table,
            })
        }
        _ => Err(ParserError::UnsupportedTableType {
            msg: table_factor.to_string(),
        }),
    }
}

fn get_join(table: &TableWithJoins) -> Result<Arc<Table>, ParserError> {
    let mut left = relation_tables(&table.relation)?;
    for join in &table.joins {
        let (left_null, right_null) = match &join.join_operator {
            JoinOperator::Inner(_) | JoinOperator::Join(_) => (false, false),
            JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => (false, true),
            JoinOperator::RightOuter(_) | JoinOperator::Right(_) => (true, false),
            JoinOperator::FullOuter(_) => (true, true),
            JoinOperator::CrossJoin => (true, true),
            JoinOperator::Semi(_)
            | JoinOperator::LeftSemi(_)
            | JoinOperator::RightSemi(_)
            | JoinOperator::Anti(_)
            | JoinOperator::LeftAnti(_)
            | JoinOperator::RightAnti(_)
            | JoinOperator::CrossApply
            | JoinOperator::OuterApply
            | JoinOperator::StraightJoin(_)
            | JoinOperator::AsOf { .. } => {
                return Err(ParserError::UnsupportedStatement {
                    statement: table.to_string(),
                });
            }
        };
        let right = relation_tables(&join.relation)?;
        left = Table::join((left_null, left), (right_null, right));
    }
    Ok(left)
}

fn identify_tables(tables: &[TableWithJoins]) -> Result<Vec<Arc<Table>>, ParserError> {
    let mut names = vec![];
    for table in tables {
        names.push(get_join(table)?);
    }
    Ok(names)
}

fn find_field_in_expr(expr: &Expr, tables: &[Arc<Table>]) -> Option<Column> {
    match expr {
        Expr::Identifier(ident) => {
            let table = tables.first()?;
            let mut result = table.find_column(&ident.value);
            for table in tables.iter().skip(1) {
                result = Column::either(result, table.find_column(&ident.value))
            }
            Some(result)
        }
        Expr::CompoundIdentifier(idents) => {
            let table_name = idents.get(idents.len() - 2);
            let (table_ident, col_ident) = table_name.zip(idents.last())?;
            let mut result = None;
            for table in tables {
                let current = table.find_table_column(&table_ident.value, &col_ident.value);
                result = match (result, current) {
                    (None, value) => value,
                    (Some(result), Some(curr)) => Some(Column::either(result, curr)),
                    _ => None,
                };
            }
            result
        }
        _ => None,
    }
}

fn find_fields_in_items(items: &[SelectItem], tables: &[Arc<Table>]) -> HashMap<String, Column> {
    let mut columns = HashMap::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let ident = match expr {
                    Expr::Identifier(ident) => Some(ident),
                    Expr::CompoundIdentifier(idents) => idents.last(),
                    _ => None,
                };
                let Some(ident) = ident else {
                    continue;
                };
                let Some(column) = find_field_in_expr(expr, tables) else {
                    continue;
                };
                columns.insert(ident.value.clone(), column);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let Some(column) = find_field_in_expr(expr, tables) else {
                    continue;
                };
                columns.insert(alias.value.clone(), column);
            }
            _ => {}
        }
    }
    columns
}

pub fn find_fields(statement: &Statement) -> Result<HashMap<String, Column>, ParserError> {
    match statement {
        Statement::Query(query) => {
            if query.with.is_some() {
                return Err(ParserError::UnsupportedQueryElement {
                    name: "with".into(),
                });
            }
            match &*query.body {
                SetExpr::Select(select) => Ok(find_fields_in_items(
                    &select.projection,
                    &identify_tables(&select.from)?,
                )),
                _ => Err(ParserError::UnsupportedStatement {
                    statement: query.to_string(),
                }),
            }
        }
        Statement::Insert(insert) => {
            let table = match &insert.table {
                TableObject::TableName(object_name) => Table::new(object_name),
                TableObject::TableFunction(_) => {
                    return Err(ParserError::UnsupportedQueryElement {
                        name: insert.table.to_string(),
                    });
                }
            };
            Ok(match &insert.returning {
                Some(returning) => find_fields_in_items(returning, &[table]),
                None => HashMap::new(),
            })
        }
        Statement::Update {
            table, returning, ..
        } => {
            let table = get_join(table)?;
            Ok(match &returning {
                Some(returning) => find_fields_in_items(returning, &[table]),
                None => HashMap::new(),
            })
        }
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithoutKeyword(tables) | FromTable::WithFromKeyword(tables) => {
                    identify_tables(tables)?
                }
            };
            Ok(match &delete.returning {
                Some(returning) => find_fields_in_items(returning, &tables),
                None => HashMap::new(),
            })
        }
        _ => Err(ParserError::UnsupportedStatement {
            statement: statement.to_string(),
        }),
    }
}

pub fn to_ast(query: &str) -> Result<Vec<Statement>, Box<dyn Error>> {
    let dialect = PostgreSqlDialect {};
    Ok(Parser::parse_sql(&dialect, query)?)
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;

    use crate::parser::{Column, find_fields, to_ast};

    const TABLES: &[&str] = &["a", "b", "c", "d", "e", "f"];
    const COLUMNS: &[&str] = &["a", "b", "c"];
    const ALIAS: &str = "x";
    const OTHER_TABLE: &str = "x";

    pub fn find_source(ast: &[Statement], field_name: &str) -> Column {
        let fields = find_fields(&ast[0]).unwrap();
        fields[field_name].clone()
    }

    #[test]
    fn basic_ident_find_source() {
        for &column in COLUMNS {
            for &table in TABLES {
                let query = format!("select {column} from {table}");
                let ast = to_ast(&query).unwrap();
                let source = find_source(&ast, column);
                assert_eq!(source, Column::depends_on(table, column));
            }
        }
    }

    #[test]
    fn compound_ident_find_source() {
        for &column in COLUMNS {
            for &table in TABLES {
                let query = format!("select {table}.{column} from {table}");
                let ast = to_ast(&query).unwrap();
                let source = find_source(&ast, column);
                assert_eq!(source, Column::depends_on(table, column));
            }
        }
    }

    #[test]
    fn basic_ident_alias_find_source() {
        for &column in COLUMNS {
            for &table in TABLES {
                let query = format!("select {column} as {ALIAS} from {table}");
                let ast = to_ast(&query).unwrap();
                let source = find_source(&ast, ALIAS);
                assert_eq!(source, Column::depends_on(table, column));
            }
        }
    }

    #[test]
    fn compound_ident_alias_find_source() {
        for &column in COLUMNS {
            for &table in TABLES {
                let query = format!("select {table}.{column} as {ALIAS} from {table}");
                let ast = to_ast(&query).unwrap();
                let source = find_source(&ast, ALIAS);
                assert_eq!(source, Column::depends_on(table, column));
            }
        }
    }

    #[test]
    fn basic_ident_find_source_with_join() {
        for &column in COLUMNS {
            for (idx, &table_a) in TABLES.iter().enumerate() {
                for &table_b in &TABLES[idx + 1..] {
                    let query = format!("select {column} from {table_a} join {table_b}");
                    let ast = to_ast(&query).unwrap();
                    let source = find_source(&ast, column);
                    assert_eq!(
                        source,
                        Column::either(
                            Column::depends_on(table_a, column),
                            Column::depends_on(table_b, column),
                        )
                    );
                }
            }
        }
    }

    #[test]
    fn compound_ident_find_source_with_join() {
        for &column in COLUMNS {
            for &table in TABLES {
                let query = format!("select {table}.{column} from {table} join {OTHER_TABLE}");
                let ast = to_ast(&query).unwrap();
                let source = find_source(&ast, column);
                assert_eq!(source, Column::depends_on(table, column));
            }
        }
    }
}
