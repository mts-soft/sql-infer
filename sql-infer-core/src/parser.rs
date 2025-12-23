use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;

use sqlparser::ast::{
    BinaryOperator, DataType, DollarQuotedString, Expr, FromTable, Function, JoinOperator,
    SelectItem, SetExpr, Statement, TableFactor, TableObject, TableWithJoins, Update,
    ValueWithSpan,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::inference::SqlType;

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
    Unknown {
        sql: String,
    },
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Table::Db { name } => write!(f, "table({name})"),
            Table::Alias { name, source } => write!(f, "alias({name}, {source})"),
            Table::Join {
                left: (left_null, left),
                right: (right_null, right),
            } => {
                write!(f, "combine(")?;
                match left_null {
                    true => write!(f, "maybe({left}), "),
                    false => write!(f, "{left}, "),
                }?;
                match right_null {
                    true => write!(f, "maybe({right})"),
                    false => write!(f, "{right}"),
                }?;
                write!(f, ")")
            }
            Table::Unknown { sql } => write!(f, "unknown({sql})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOpData {
    Unknown {
        inner: BinaryOperator,
    },
    ConstantType {
        inner: BinaryOperator,
        sql_type: SqlType,
    },
    Numeric {
        inner: BinaryOperator,
    },
    Concat,
}

impl BinaryOpData {
    fn unknown(op: BinaryOperator) -> Self {
        Self::Unknown { inner: op }
    }

    fn constant(op: BinaryOperator, sql_type: SqlType) -> Self {
        Self::ConstantType {
            inner: op,
            sql_type,
        }
    }

    fn numeric(op: BinaryOperator) -> Self {
        Self::Numeric { inner: op }
    }

    fn concat() -> Self {
        Self::Concat
    }

    /// Returns boolean indicating whether the output is guaranteed to be not null regardless of arguments.
    pub fn not_null(&self) -> Option<bool> {
        Some(false)
    }

    /// Returns type if the output of this operation is a single type regardless of the arguments
    pub fn try_constant(&self) -> Option<SqlType> {
        match self {
            BinaryOpData::ConstantType { sql_type, .. } => Some(sql_type.clone()),
            _ => None,
        }
    }

    /// Returns type if the output of this operation can be determined
    pub fn try_from_operands(&self, left: SqlType, right: SqlType) -> Option<SqlType> {
        match self {
            BinaryOpData::Unknown { .. } => None,
            BinaryOpData::ConstantType { sql_type, .. } => Some(sql_type.clone()),
            BinaryOpData::Numeric { .. } => {
                if !(left.is_numeric() || right.is_numeric()) {
                    return None;
                }
                match left.numeric_compare(&right)? {
                    std::cmp::Ordering::Greater => Some(left),
                    _ => Some(right),
                }
            }
            BinaryOpData::Concat => {
                if left.is_text() || right.is_text() {
                    return Some(SqlType::Text);
                }
                None
            }
        }
    }
}

impl From<BinaryOperator> for BinaryOpData {
    fn from(value: BinaryOperator) -> Self {
        // https://www.postgresql.org/docs/current/functions-math.html
        match &value {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => BinaryOpData::numeric(value),
            BinaryOperator::StringConcat => BinaryOpData::concat(),
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Xor => BinaryOpData::constant(value, SqlType::Bool),
            _ => BinaryOpData::unknown(value),
        }
    }
}

impl Display for BinaryOpData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOpData::Unknown { inner } | BinaryOpData::Numeric { inner } => {
                write!(f, "{inner}")
            }
            BinaryOpData::ConstantType { inner, sql_type } => {
                write!(f, "op({inner}) -> {sql_type}")
            }
            BinaryOpData::Concat => write!(f, "concat"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValueType {
    Boolean,
    Int,
    Float,
    String,
    Null,
}

impl Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Boolean => write!(f, "bool"),
            ValueType::Int => write!(f, "int"),
            ValueType::Float => write!(f, "float"),
            ValueType::String => write!(f, "string"),
            ValueType::Null => write!(f, "null"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
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
    Unknown {
        sql: String,
    },
    Cast {
        source: Arc<Column>,
        data_type: DataType,
    },
    BinaryOp {
        op: BinaryOpData,
        left: Arc<Column>,
        right: Arc<Column>,
    },
    Value(ValueType),
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::DependsOn { table, column } => write!(f, "{table}.{column}"),
            Column::Maybe { column } => write!(f, "maybe({column})"),
            Column::Either { left, right } => write!(f, "either({left}, {right})"),
            Column::Unknown { sql } => write!(f, "unknown({sql})"),
            Column::Cast { source, data_type } => write!(f, "cast({source}, {data_type})"),
            Column::BinaryOp { op, left, right } => write!(f, "binop({op}, {left}, {right})"),
            Column::Value(value) => write!(f, "{value}"),
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

    pub fn cast(self, data_type: DataType) -> Self {
        Column::Cast {
            source: self.into(),
            data_type,
        }
    }

    pub fn bin_op(op: impl Into<BinaryOpData>, left: Column, right: Column) -> Self {
        Column::BinaryOp {
            op: op.into(),
            left: left.into(),
            right: right.into(),
        }
    }

    pub fn value(value: ValueType) -> Self {
        Self::Value(value)
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

    pub fn unknown(sql: String) -> Arc<Self> {
        Self::Unknown { sql }.into()
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
            Table::Unknown { sql } => Some(Column::Unknown { sql: sql.clone() }),
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
            Table::Unknown { sql } => Column::Unknown { sql: sql.clone() },
        }
    }
}

fn unescape(name: &str) -> String {
    if !name.starts_with("\"") || !name.ends_with("\"") {
        return name.to_string();
    }
    name[1..name.len() - 1].replace("\"\"", "\"")
}

fn relation_tables(table_factor: &TableFactor) -> Arc<Table> {
    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            let table = Table::new(unescape(&name.to_string()));
            match alias {
                Some(alias) => Table::alias(alias, table),
                None => table,
            }
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let table = get_join(table_with_joins);
            match alias {
                Some(alias) => Table::alias(alias, table),
                None => table,
            }
        }
        _ => Table::unknown(table_factor.to_string()),
    }
}

fn get_join(table: &TableWithJoins) -> Arc<Table> {
    let mut left = relation_tables(&table.relation);
    for join in &table.joins {
        let (left_null, right_null) = match &join.join_operator {
            JoinOperator::Inner(_) | JoinOperator::Join(_) => (false, false),
            JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => (false, true),
            JoinOperator::RightOuter(_) | JoinOperator::Right(_) => (true, false),
            JoinOperator::FullOuter(_) => (true, true),
            JoinOperator::CrossJoin(_) => (true, true),
            JoinOperator::Semi(_)
            | JoinOperator::LeftSemi(_)
            | JoinOperator::RightSemi(_)
            | JoinOperator::Anti(_)
            | JoinOperator::LeftAnti(_)
            | JoinOperator::RightAnti(_)
            | JoinOperator::CrossApply
            | JoinOperator::OuterApply
            | JoinOperator::StraightJoin(_)
            | JoinOperator::AsOf { .. } => return Table::unknown(join.to_string()),
        };
        let right = relation_tables(&join.relation);
        left = Table::join((left_null, left), (right_null, right));
    }
    left
}

fn identify_tables(tables: &[TableWithJoins]) -> Vec<Arc<Table>> {
    tables.iter().map(get_join).collect()
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
        Expr::Cast {
            expr, data_type, ..
        } => Some(find_field_in_expr(expr, tables)?.cast(data_type.clone())),
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
        Expr::Nested(expr) => find_field_in_expr(expr, tables),
        Expr::BinaryOp { left, op, right } => Some(Column::bin_op(
            op.clone(),
            find_field_in_expr(left, tables)?,
            find_field_in_expr(right, tables)?,
        )),
        Expr::Value(ValueWithSpan { value, .. }) => {
            use sqlparser::ast::Value;
            match value {
                Value::Number(number, _) => Some(match number.is_integer() {
                    true => Column::value(ValueType::Int),
                    false => Column::value(ValueType::Float),
                }),
                Value::SingleQuotedString(_string)
                | Value::DollarQuotedString(DollarQuotedString { value: _string, .. })
                | Value::TripleSingleQuotedString(_string)
                | Value::TripleDoubleQuotedString(_string)
                | Value::EscapedStringLiteral(_string)
                | Value::UnicodeStringLiteral(_string)
                | Value::SingleQuotedByteStringLiteral(_string)
                | Value::DoubleQuotedByteStringLiteral(_string)
                | Value::TripleSingleQuotedByteStringLiteral(_string)
                | Value::TripleDoubleQuotedByteStringLiteral(_string)
                | Value::SingleQuotedRawStringLiteral(_string)
                | Value::DoubleQuotedRawStringLiteral(_string)
                | Value::TripleSingleQuotedRawStringLiteral(_string)
                | Value::TripleDoubleQuotedRawStringLiteral(_string)
                | Value::NationalStringLiteral(_string)
                | Value::HexStringLiteral(_string)
                | Value::DoubleQuotedString(_string) => Some(Column::value(ValueType::String)),
                Value::Boolean(_boolean) => Some(Column::Value(ValueType::Boolean)),
                Value::Null => Some(Column::Value(ValueType::Null)),
                Value::Placeholder(_) => None,
            }
        }
        Expr::Function(Function { name, .. }) if name.to_string().to_lowercase() == "count" => {
            Some(Column::Value(ValueType::Int))
        }
        _ => Some(Column::Unknown {
            sql: expr.to_string(),
        }),
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

pub fn find_tables(statement: &Statement) -> Vec<Arc<Table>> {
    match statement {
        Statement::Query(query) => match &*query.body {
            SetExpr::Select(select) => identify_tables(&select.from),
            _ => vec![Table::unknown(query.to_string())],
        },
        Statement::Insert(insert) => {
            let table = match &insert.table {
                TableObject::TableName(object_name) => {
                    Table::new(unescape(&object_name.to_string()))
                }
                _ => Table::unknown(insert.table.to_string()),
            };
            vec![table]
        }
        Statement::Update(Update { table, .. }) => vec![get_join(table)],
        Statement::Delete(delete) => match &delete.from {
            FromTable::WithoutKeyword(tables) | FromTable::WithFromKeyword(tables) => {
                identify_tables(tables)
            }
        },
        _ => vec![Table::unknown(statement.to_string())],
    }
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
                    &identify_tables(&select.from),
                )),
                _ => Err(ParserError::UnsupportedStatement {
                    statement: query.to_string(),
                }),
            }
        }
        Statement::Insert(insert) => {
            let table = match &insert.table {
                TableObject::TableName(object_name) => {
                    Table::new(unescape(&object_name.to_string()))
                }
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
        Statement::Update(Update {
            table, returning, ..
        }) => {
            let table = get_join(table);
            Ok(match &returning {
                Some(returning) => find_fields_in_items(returning, &[table]),
                None => HashMap::new(),
            })
        }
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithoutKeyword(tables) | FromTable::WithFromKeyword(tables) => {
                    identify_tables(tables)
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
