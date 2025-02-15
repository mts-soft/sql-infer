use std::error::Error;
use std::fmt::Display;

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
    NoCandidateTables { item: String },
    WildcardsNotSupported,
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
            ParserError::WildcardsNotSupported => write!(f, "Wildcards are not supported"),
            ParserError::NoCandidateTables { item } => {
                write!(f, "No candidate tables for item {item}")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbTable {
    pub name: String,
    pub nullable: bool,
}

impl DbTable {
    pub fn new(name: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            nullable,
        }
    }
}

fn relation_tables(table_factor: &TableFactor) -> Result<Vec<DbTable>, ParserError> {
    match table_factor {
        TableFactor::Table { name, .. } => Ok(vec![DbTable::new(name.to_string(), false)]),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => identify_table(table_with_joins),
        _ => Err(ParserError::UnsupportedTableType {
            msg: table_factor.to_string(),
        }),
    }
}

fn identify_table(table: &TableWithJoins) -> Result<Vec<DbTable>, ParserError> {
    let mut tables = relation_tables(&table.relation)?;
    for join in &table.joins {
        let (left, right) = match &join.join_operator {
            JoinOperator::Inner(_) => (false, false),
            JoinOperator::LeftOuter(_) => (false, true),
            JoinOperator::RightOuter(_) => (true, false),
            JoinOperator::FullOuter(_) => (true, true),
            JoinOperator::CrossJoin => (true, true),
            _ => {
                return Err(ParserError::UnsupportedStatement {
                    statement: table.to_string(),
                })
            }
        };
        let mut right_tables = relation_tables(&join.relation)?;
        tables.iter_mut().for_each(|table| table.nullable |= left);
        right_tables
            .iter_mut()
            .for_each(|table| table.nullable |= right);
        tables.extend(right_tables);
    }
    Ok(tables)
}

fn identify_tables(tables: &[TableWithJoins]) -> Result<Vec<DbTable>, ParserError> {
    let mut names = vec![];
    for table in tables {
        names.extend(identify_table(table)?);
    }
    Ok(names)
}

fn find_field(
    items: &[SelectItem],
    tables: &[DbTable],
    field: &str,
) -> Result<Option<SourceColumn>, ParserError> {
    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let table = if tables.is_empty() {
                        return Err(ParserError::NoCandidateTables {
                            item: ident.value.clone(),
                        });
                    } else if tables.len() > 1 {
                        None
                    } else {
                        tables.first().cloned()
                    };
                    if ident.value == field {
                        return Ok(Some(SourceColumn {
                            table,
                            column: ident.value.clone(),
                        }));
                    }
                }
                Expr::CompoundIdentifier(idents) => {
                    /*
                    schema.table.col or table.col are the two possibilities.
                    len() - 1 gets us the table name.
                     */
                    let table_name = idents.get(idents.len() - 2);
                    if let Some((table_name, item_name)) = table_name.zip(idents.last()) {
                        let table = tables.iter().find(|table| table.name == table_name.value);

                        return Ok(Some(SourceColumn {
                            table: table.cloned(),
                            column: item_name.value.clone(),
                        }));
                    }
                }
                _ => {
                    return Err(ParserError::UnsupportedStatement {
                        statement: expr.to_string(),
                    })
                }
            },
            SelectItem::ExprWithAlias { .. } => {
                return Err(ParserError::UnsupportedStatement {
                    statement: item.to_string(),
                })
            }
            SelectItem::QualifiedWildcard(_, _) => return Err(ParserError::WildcardsNotSupported),
            SelectItem::Wildcard(_) => return Err(ParserError::WildcardsNotSupported),
        }
    }
    Ok(None)
}

#[derive(Debug, Clone)]
pub struct SourceColumn {
    pub table: Option<DbTable>,
    pub column: String,
}

impl Display for SourceColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Column \"{}\" from table \"{}\"",
            self.column,
            self.table
                .as_ref()
                .map(|table| format!("{} guaranteed: {}", table.name, !table.nullable))
                .unwrap_or("???".to_owned())
        )
    }
}

pub fn to_ast(query: &str) -> Result<Vec<Statement>, Box<dyn Error>> {
    let dialect = PostgreSqlDialect {};
    Ok(Parser::parse_sql(&dialect, query)?)
}

pub fn find_source(
    ast: &[Statement],
    field_name: &str,
) -> Result<Option<SourceColumn>, ParserError> {
    for token in ast {
        match token {
            Statement::Query(query) => {
                if query.with.is_some() {
                    return Err(ParserError::UnsupportedQueryElement {
                        name: "with".into(),
                    });
                }
                match &*query.body {
                    SetExpr::Select(select) => {
                        return find_field(
                            &select.projection,
                            &identify_tables(&select.from)?,
                            field_name,
                        );
                    }
                    _ => {
                        return Err(ParserError::UnsupportedStatement {
                            statement: query.to_string(),
                        })
                    }
                }
            }
            Statement::Insert(insert) => {
                let table = match &insert.table {
                    TableObject::TableName(object_name) => {
                        DbTable::new(object_name.to_string(), false)
                    }
                    TableObject::TableFunction(_) => {
                        return Err(ParserError::UnsupportedQueryElement {
                            name: insert.table.to_string(),
                        })
                    }
                };
                if let Some(returning) = &insert.returning {
                    return find_field(returning, &[table], field_name);
                }
            }
            Statement::Update {
                table, returning, ..
            } => {
                let tables = identify_table(table)?;
                if let Some(returning) = &returning {
                    return find_field(returning, &tables, field_name);
                }
            }
            Statement::Delete(delete) => {
                let tables = match &delete.from {
                    FromTable::WithoutKeyword(tables) | FromTable::WithFromKeyword(tables) => {
                        identify_tables(tables)?
                    }
                };
                if let Some(returning) = &delete.returning {
                    return find_field(returning, &tables, field_name);
                }
            }
            _ => {
                return Err(ParserError::UnsupportedStatement {
                    statement: token.to_string(),
                })
            }
        }
    }
    Ok(None)
}
