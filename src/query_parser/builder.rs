use crate::query_parser::query::{Column, ColumnType, Condition, DataManipulationQuery, DeleteQuery, InsertQuery, Operator, Query, SelectQuery, UpdateQuery, Value};

pub(crate) struct ColumnBuilder {
    column_name: Option<String>,
    column_type: Option<ColumnType>,
}

pub(crate) struct SelectQueryBuilder {
    columns: Vec<String>,
    table: Option<String>,
    conditions: Vec<Condition>,
}

pub(crate) struct InsertQueryBuilder {
    columns: Vec<String>,
    table: Option<String>,
    values: Vec<Value>,
}

pub(crate) struct UpdateQueryBuilder {
    table: Option<String>,
    values: Vec<(String, Value)>,
    conditions: Vec<Condition>,
}

pub(crate) struct DeleteQueryBuilder {
    columns: Vec<String>,
    table: Option<String>,
    conditions: Vec<Condition>,
}

pub(crate) struct ConditionBuilder {
    column: Option<String>,
    operator: Option<Operator>,
    value: Option<Value>,
}
impl ColumnBuilder {
    #[inline]
    pub(crate) fn new() -> Self {
        ColumnBuilder {
            column_name: None,
            column_type: None,
        }
    }

    #[inline]
    pub(crate) fn name(mut self, name: String) -> Self {
        self.column_name = Some(name);
        self
    }

    #[inline]
    pub(crate) fn column_type(mut self, column_type: ColumnType) -> Self {
        self.column_type = Some(column_type);
        self
    }

    #[inline]
    pub(crate) fn build(self) -> Column {
        Column {
            name: self.column_name.expect("column_name field doesn't set"),
            column_type: self.column_type.expect("column_type field doesn't set"),
        }
    }
}

impl SelectQueryBuilder {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            columns: Vec::default(),
            table: None,
            conditions: Vec::default(),
        }
    }

    #[inline]
    pub(crate) fn column(mut self, column: String) -> Self {
        self.columns.push(column);
        self
    }

    #[inline]
    pub(crate) fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns.extend(columns);
        self
    }

    #[inline]
    pub(crate) fn table(mut self, table: String) -> Self {
        self.table = Some(table);
        self
    }

    #[inline]
    pub(crate) fn condition(mut self, column: Condition) -> Self {
        self.conditions.push(column);
        self
    }

    #[inline]
    pub(crate) fn conditions(mut self, conditions: Vec<Condition>) -> Self {
        self.conditions.extend(conditions);
        self
    }

    #[inline]
    pub(crate) fn build(self) -> Query {
        Query::DataManipulationQuery(DataManipulationQuery::Select(SelectQuery::new(
            self.columns,
            self.table.expect("the table doesn't set"),
            self.conditions,
        )))
    }
}

impl InsertQueryBuilder {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            columns: Vec::default(),
            table: None,
            values: Vec::default(),
        }
    }

    #[inline]
    pub(crate) fn column(mut self, column: String) -> Self {
        self.columns.push(column);
        self
    }

    #[inline]
    pub(crate) fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns.extend(columns);
        self
    }

    #[inline]
    pub(crate) fn table(mut self, table: String) -> Self {
        self.table = Some(table);
        self
    }

    #[inline]
    pub(crate) fn value(mut self, value: Value) -> Self {
        self.values.push(value);
        self
    }

    #[inline]
    pub(crate) fn values(mut self, values: Vec<Value>) -> Self {
        self.values.extend(values);
        self
    }

    #[inline]
    pub(crate) fn build(self) -> Query {
        Query::DataManipulationQuery(DataManipulationQuery::Insert(InsertQuery::new(
            self.columns,
            self.table.expect("the table doesn't set"),
            self.values,
        )))
    }
}

impl UpdateQueryBuilder {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            values: Vec::default(),
            table: None,
            conditions: Vec::default(),
        }
    }

    #[inline]
    pub(crate) fn value(mut self, value: (String, Value)) -> Self {
        self.values.push(value);
        self
    }

    #[inline]
    pub(crate) fn values(mut self, values: Vec<(String, Value)>) -> Self {
        self.values.extend(values);
        self
    }

    #[inline]
    pub(crate) fn table(mut self, table: String) -> Self {
        self.table = Some(table);
        self
    }

    #[inline]
    pub(crate) fn condition(mut self, column: Condition) -> Self {
        self.conditions.push(column);
        self
    }

    #[inline]
    pub(crate) fn conditions(mut self, conditions: Vec<Condition>) -> Self {
        self.conditions.extend(conditions);
        self
    }

    #[inline]
    pub(crate) fn build(self) -> Query {
        Query::DataManipulationQuery(DataManipulationQuery::Update(UpdateQuery::new(
            self.table.expect("the table doesn't set"),
            self.values,
            self.conditions,
        )))
    }
}

impl DeleteQueryBuilder {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            columns: Vec::default(),
            table: None,
            conditions: Vec::default(),
        }
    }

    #[inline]
    pub(crate) fn column(mut self, column: String) -> Self {
        self.columns.push(column);
        self
    }

    #[inline]
    pub(crate) fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns.extend(columns);
        self
    }

    #[inline]
    pub(crate) fn table(mut self, table: String) -> Self {
        self.table = Some(table);
        self
    }

    #[inline]
    pub(crate) fn condition(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }

    #[inline]
    pub(crate) fn conditions(mut self, condition: Vec<Condition>) -> Self {
        self.conditions.extend(condition);
        self
    }

    #[inline]
    pub(crate) fn build(self) -> Query {
        Query::DataManipulationQuery(DataManipulationQuery::Delete(DeleteQuery::new(
            self.columns,
            self.table.expect("the table doesn't set"),
            self.conditions,
        )))
    }
}

impl ConditionBuilder {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            column: None,
            operator: None,
            value: None,
        }
    }

    #[inline]
    pub(crate) fn column(mut self, column: String) -> Self {
        self.column = Some(column);
        self
    }

    #[inline]
    pub(crate) fn operator(mut self, operator: Operator) -> Self {
        self.operator = Some(operator);
        self
    }

    #[inline]
    pub(crate) fn value(mut self, value: Value) -> Self {
        self.value = Some(value);
        self
    }

    #[inline]
    pub(crate) fn build(self) -> Condition {
        Condition {
            column: self.column.expect("the column doesn't set"),
            operator: self.operator.expect("the operator doesn't set"),
            value: self.value.expect("the value doesn't set"),
        }
    }
}

