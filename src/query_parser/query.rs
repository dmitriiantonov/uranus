use std::fmt::Debug;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Query {
    DataManipulationQuery(DataManipulationQuery),
    DataDefinitionQuery(DataDefinitionQuery),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DataManipulationQuery {
    Select(SelectQuery),
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DataDefinitionQuery {
    CreateTable(CreateTableQuery),
    AlterTable(AlterTableQuery),
    DropTable(DropTableQuery),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Operator {
    Equals,
    NotEquals,
    Greater,
    GreaterOrEquals,
    Less,
    LessOrEquals,
}

#[derive(Debug, Eq, PartialEq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    AlterTable,
    DropTable
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SelectQuery {
    pub(crate) columns: Vec<String>,
    pub(crate) table: String,
    pub(crate) conditions: Vec<Condition>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct InsertQuery {
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<Value>,
    pub(crate) table: String,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct UpdateQuery {
    pub(crate) table: String,
    pub(crate) values: Vec<(String, Value)>,
    pub(crate) conditions: Vec<Condition>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DeleteQuery {
    pub(crate) columns: Vec<String>,
    pub(crate) table: String,
    pub(crate) conditions: Vec<Condition>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CreateTableQuery {
    pub(crate) table: String,
    pub(crate) primary_key: PrimaryKey,
    pub(crate) columns: Vec<Column>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PrimaryKey {
    pub(crate) partition_key: Vec<String>,
    pub(crate) clustering_key: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AlterTableQuery {
    pub(crate) table: String,
    pub(crate) conditions: Vec<AlterTableCondition>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DropTableQuery {
    pub(crate) table: String,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Condition {
    pub(crate) column: String,
    pub(crate) operator: Operator,
    pub(crate) value: Value,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AlterTableCondition {
    AddColumn(AddColumnCondition),
    DropColumn(DropColumnCondition),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AddColumnCondition {
    pub(crate) column_name: String,
    pub(crate) column_type: ColumnType,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DropColumnCondition {
    pub(crate) column_name: String,
}

#[derive(Debug)]
pub(crate) enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ColumnType {
    Uuid,
    Int,
    Long,
    Float,
    Double,
    Timestamp,
    Text,
    Bool,
}

impl SelectQuery {
    pub(crate) fn new(columns: Vec<String>, table: String, conditions: Vec<Condition>) -> Self {
        Self { columns, table, conditions }
    }
}

impl InsertQuery {
    pub(crate) fn new(columns: Vec<String>, table: String, values: Vec<Value>) -> Self {
        Self { columns, table, values }
    }
}

impl UpdateQuery {
    pub(crate) fn new(table: String, values: Vec<(String, Value)>, conditions: Vec<Condition>) -> Self {
        Self { table, values, conditions }
    }
}

impl DeleteQuery {
    pub(crate) fn new(columns: Vec<String>, table: String, conditions: Vec<Condition>) -> Self {
        Self { columns, table, conditions }
    }
}

impl Condition {
    pub(crate) fn new(column: String, operator: Operator, value: Value) -> Self {
        Self { column, operator, value }
    }
}

#[derive(Debug)]
#[derive(PartialEq)]
pub(crate) enum QueryParsingError {
    UnsupportedRequest(String),
    QuerySyntaxError(String, String),
}

impl Eq for Value {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(x), Value::Integer(y)) => x == y,
            (Value::Float(x), Value::Float(y)) => f64::eq(x, y),
            (Value::String(x), Value::String(y)) => x.eq(y),
            (Value::Bool(x), Value::Bool(y)) => x == y,
            _ => false
        }
    }
}