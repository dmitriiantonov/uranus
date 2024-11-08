use crate::query_processor::dml_parser::keyword::*;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_while1};
use nom::character::complete::{digit1, i64 as parse_i64, multispace0};
use nom::combinator::{map, map_res, opt, recognize};
use nom::multi::separated_list1;
use nom::sequence::{delimited, tuple};
use nom::{IResult};
use std::error::Error;
use std::fmt::{Display, Formatter};

mod keyword {
    pub(super) const SELECT: &str = "SELECT";
    pub(super) const INSERT_INTO: &str = "INSERT INTO";
    pub(super) const UPDATE: &str = "UPDATE";
    pub(super) const FROM: &str = "FROM";
    pub(super) const WHERE: &str = "WHERE";
    pub(super) const AND: &str = "AND";
    pub(super) const VALUES: &str = "VALUES";
    pub(super) const SET: &str = "SET";
    pub (super) const DELETE: &str = "DELETE";
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SelectQuery {
    columns: Vec<String>,
    table: String,
    conditions: Vec<Condition>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct InsertQuery {
    columns: Vec<String>,
    values: Vec<Value>,
    table: String,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct UpdateQuery {
    table: String,
    values: Vec<(String, Value)>,
    conditions: Vec<Condition>,
}

#[derive(Debug, Eq, PartialEq)]
pub (crate) struct DeleteQuery {
    columns: Vec<String>,
    table: String,
    conditions: Vec<Condition>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Condition {
    column: String,
    operator: Operator,
    value: Value,
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

#[derive(Debug)]
pub(crate) enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

enum QueryType {
    Select,
    Insert,
    Update,
    Delete
}

impl SelectQuery {
    fn new(columns: Vec<String>, table: String, conditions: Vec<Condition>) -> Self {
        Self { columns, table, conditions }
    }
}

impl InsertQuery {
    fn new(columns: Vec<String>, table: String, values: Vec<Value>) -> Self {
        Self { columns, table, values }
    }
}

impl UpdateQuery {
    fn new(table: String, values: Vec<(String, Value)>, conditions: Vec<Condition>) -> Self {
        Self { table, values, conditions }
    }
}

impl DeleteQuery {
    fn new(columns: Vec<String>, table: String, conditions: Vec<Condition>) -> Self {
        Self { columns, table, conditions }
    }
}

impl Condition {
    fn new(column: String, operator: Operator, value: Value) -> Self {
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

impl Display for QueryParsingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryParsingError::UnsupportedRequest(query) => write!(f, "the request {} is not supported", query),
            QueryParsingError::QuerySyntaxError(msg, query) => write!(f, "an syntax error {} occurred while parsing the request {}", msg, query)
        }
    }
}

impl Error for QueryParsingError {}

pub(crate) fn parse_query(query: &str) -> Result<Query, QueryParsingError> {
    let query_type = get_query_type(query)?;

    match query_type {
        QueryType::Select => parse_select_query(query),
        QueryType::Insert => parse_insert(query),
        QueryType::Update => parse_update(query),
        QueryType::Delete => parse_delete(query),
    }
}

fn get_query_type(query: &str) -> Result<QueryType, QueryParsingError> {
    let query_type_result: IResult<&str, QueryType> = alt((
        map(parse_keyword(SELECT), |_| QueryType::Select),
        map(parse_keyword(INSERT_INTO), |_| QueryType::Insert),
        map(parse_keyword(UPDATE), |_| QueryType::Update),
        map(parse_keyword(DELETE), |_| QueryType::Delete),
    ))(query);

    match query_type_result {
        Ok((_, query_type)) => Ok(query_type),
        Err(_) => Err(QueryParsingError::UnsupportedRequest(query.to_string()))
    }
}

fn parse_select_query(query: &str) -> Result<Query, QueryParsingError> {
    let query = match parse_keyword(SELECT)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the select keyword".to_string(), query.to_string()))
    };

    let parsing_result: IResult<&str, Vec<String>> = alt((
        map(
            ws(tag("*")),
            |_| Vec::new(),
        ),
        separated_list1(
            ws(tag(",")),
            parse_identifier,
        )
    ))(query);

    let (query, columns) = match parsing_result {
        Ok((query, columns)) => (query, columns),
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the column names or *".to_string(), query.to_string()))
    };

    let query = match parse_keyword(FROM)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the from keyword".to_string(), query.to_string()))
    };

    let (query, table) = match parse_identifier(query) {
        Ok((query, table)) => (query, table),
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the table name".to_string(), query.to_string()))
    };

    let conditions = match parse_conditions(query) {
        Ok((_, conditions)) => conditions,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing where condition".to_string(), query.to_string()))
    };

    Ok(Query::Select(SelectQuery::new(columns, table, conditions)))
}

fn parse_conditions(query: &str) -> IResult<&str, Vec<Condition>> {
    match parse_keyword(WHERE)(query) {
        Ok((query, _)) => separated_list1(parse_keyword(AND), parse_condition)(query),
        Err(_) => Ok((query, Vec::new()))
    }
}

fn parse_condition(query: &str) -> IResult<&str, Condition> {
    let (query, column) = parse_identifier(query)?;

    let (query, operator) = alt((
        map(ws(tag(">=")), |_| Operator::GreaterOrEquals),
        map(ws(tag("<=")), |_| Operator::LessOrEquals),
        map(ws(tag(">")), |_| Operator::Greater),
        map(ws(tag("<")), |_| Operator::Less),
        map(ws(tag("=")), |_| Operator::Equals),
        map(ws(tag("!=")), |_| Operator::NotEquals),
    ))(query)?;

    let (query, value) = alt((
        parse_float,
        parse_integer,
        map(ws(tag("false")), |_| Value::Bool(false)),
        map(ws(tag("true")), |_| Value::Bool(true)),
        parse_string
    ))(query)?;

    Ok((query, Condition { column, operator, value }))
}

fn parse_insert(query: &str) -> Result<Query, QueryParsingError> {
    let query = match parse_keyword(INSERT_INTO)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the insert into keyword".to_string(), query.to_string()))
    };

    let (query, table) = match parse_identifier(query) {
        Ok((query, table)) => (query, table),
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the table name".to_string(), query.to_string()))
    };

    let parsing_result = ws(delimited(
        tag("("),
        separated_list1(ws(tag(",")), parse_identifier),
        tag(")"),
    ))(query);

    let (query, columns) = match parsing_result {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing column names".to_string(), query.to_string()))
    };

    let query = match parse_keyword(VALUES)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the values keyword".to_string(), query.to_string()))
    };

    let parsing_result = ws(delimited(
        tag("("),
        separated_list1(ws(tag(",")), parse_value),
        tag(")"),
    ))(query);

    let (_, values) = match parsing_result {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing values".to_string(), query.to_string()))
    };

    Ok(Query::Insert(InsertQuery::new(columns, table, values)))
}

fn parse_update(query: &str) -> Result<Query, QueryParsingError> {
    let query = match parse_keyword(UPDATE)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing update keyword".to_string(), query.to_string()))
    };

    let (query, table) = match parse_identifier(query) {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing the table name".to_string(), query.to_string()))
    };

    let query = match parse_keyword(SET)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected set keyword".to_string(), query.to_string()))
    };

    let (query, values) = match separated_list1(
        ws(tag(",")),
        map(
            tuple((parse_identifier, ws(tag("=")), parse_value)),
            |(column, _, value)| (column, value),
        ),
    )(query) {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing values".to_string(), query.to_string()))
    };

    let conditions = match parse_conditions(query) {
        Ok((_, conditions)) => conditions,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing where condition".to_string(), query.to_string()))
    };

    Ok(Query::Update(UpdateQuery::new(table, values, conditions)))
}

fn parse_delete(query: &str) -> Result<Query, QueryParsingError> {
    let query = match parse_keyword(DELETE)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing delete keyword".to_string(), query.to_string()))
    };
    
    let (query, columns) = match parse_keyword(FROM)(query) {
        Ok((query,_)) => (query, Vec::new()),
        Err(_) => match separated_list1(ws(tag(",")), parse_identifier)(query) {
            Ok((query, columns)) => match parse_keyword(FROM)(query) {
                Ok((query, _)) => (query, columns),
                Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing from keyword".to_string(), query.to_string()))
            },
            Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing the columns".to_string(), query.to_string()))
        }
    };

    let (query, table) = match parse_identifier(query) {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing the table name".to_string(), query.to_string()))
    };

    let conditions = match parse_conditions(query) {
        Ok((_, conditions)) => conditions,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing where condition".to_string(), query.to_string()))
    };

    Ok(Query::Delete(DeleteQuery::new(columns, table, conditions)))
}

fn parse_keyword<'a>(keyword: &'a str) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str> {
    ws(tag_no_case(keyword))
}

fn parse_value(input: &str) -> IResult<&str, Value> {
    alt((
        parse_float,
        parse_integer,
        map(ws(tag("false")), |_| Value::Bool(false)),
        map(ws(tag("true")), |_| Value::Bool(true)),
        parse_string
    ))(input)
}

fn parse_string(input: &str) -> IResult<&str, Value> {
    let string_parser = ws(delimited(tag("'"), take_while1(|ch: char| ch != '\''), tag("'")));
    map(string_parser, |string: &str| Value::String(string.to_string()))(input)
}

fn parse_float(input: &str) -> IResult<&str, Value> {
    ws(map_res(
        recognize(tuple((opt(tag("-")), digit1, tag("."), digit1))),
        |s: &str| { s.parse::<f64>().map(Value::Float) },
    ))(input)
}

fn parse_integer(input: &str) -> IResult<&str, Value> {
    ws(map(parse_i64, Value::Integer))(input)
}

fn parse_identifier(input: &str) -> IResult<&str, String> {
    let filter = |ch: char| -> bool {
        ch.is_alphabetic() || ch == '_'
    };
    ws(map(take_while1(filter), String::from))(input)
}

pub fn ws<'a, F: 'a, O>(f: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O>,
{
    delimited(multispace0, f, multispace0)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_select_all() {
        let query = "select * from sensors";
        assert_eq!(parse_query(query), Ok(Query::Select(SelectQuery::new(vec![], "sensors".to_string(), vec![]))));
    }

    #[test]
    fn test_select_all_formated() {
        let query = "select * \n\rfrom sensors";
        assert_eq!(parse_query(query), Ok(Query::Select(SelectQuery::new(vec![], "sensors".to_string(), vec![]))));
    }

    #[test]
    fn test_select_with_where() {
        let query = "select sensor_id, timestamp, temperature from sensors where sensor_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f' and timestamp >= '2024-10-21 00:00:00' and timestamp <= '2024-11-01 00:00:00'";
        let expected_result = SelectQuery::new(
            vec!["sensor_id".to_string(), "timestamp".to_string(), "temperature".to_string()],
            "sensors".to_string(),
            vec![
                Condition::new("sensor_id".to_string(), Operator::Equals, Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string())),
                Condition::new("timestamp".to_string(), Operator::GreaterOrEquals, Value::String("2024-10-21 00:00:00".to_string())),
                Condition::new("timestamp".to_string(), Operator::LessOrEquals, Value::String("2024-11-01 00:00:00".to_string())),
            ],
        );
        assert_eq!(parse_query(query), Ok(Query::Select(expected_result)));
    }

    #[test]
    fn test_select_with_where_formated() {
        let query = "select sensor_id, timestamp, temperature \
        from sensors \
        where sensor_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f' \
        and timestamp >= '2024-10-21 00:00:00' \
        and timestamp <= '2024-11-01 00:00:00'";

        let expected_result = SelectQuery::new(
            vec!["sensor_id".to_string(), "timestamp".to_string(), "temperature".to_string()],
            "sensors".to_string(),
            vec![
                Condition::new("sensor_id".to_string(), Operator::Equals, Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string())),
                Condition::new("timestamp".to_string(), Operator::GreaterOrEquals, Value::String("2024-10-21 00:00:00".to_string())),
                Condition::new("timestamp".to_string(), Operator::LessOrEquals, Value::String("2024-11-01 00:00:00".to_string())),
            ],
        );

        assert_eq!(parse_query(query), Ok(Query::Select(expected_result)));
    }

    #[test]
    fn test_integer_and_float_parse_correctly() {
        let query = "select * from products where quantity > 10 and price > 30.65 and type != 'book'";

        let expected_result = SelectQuery::new(
            vec![],
            "products".to_string(),
            vec![
                Condition::new("quantity".to_string(), Operator::Greater, Value::Integer(10)),
                Condition::new("price".to_string(), Operator::Greater, Value::Float(30.65)),
                Condition::new("type".to_string(), Operator::NotEquals, Value::String("book".to_string())),
            ],
        );

        assert_eq!(parse_query(query), Ok(Query::Select(expected_result)));
    }

    #[test]
    fn test_parse_insert_query() {
        let query = "insert into user_sessions (user_id, session_id, type, device_type, timestamp)\
        values (12345, '3e3be9fb-5888-4b0e-8f22-287b7d90a32f', 'LOG_IN', 'PHONE', '2024-11-01 00:00:00')";

        let expected_result = InsertQuery::new(
            vec![
                "user_id".to_string(),
                "session_id".to_string(),
                "type".to_string(),
                "device_type".to_string(),
                "timestamp".to_string(),
            ],
            "user_sessions".to_string(),
            vec![
                Value::Integer(12345),
                Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string()),
                Value::String("LOG_IN".to_string()),
                Value::String("PHONE".to_string()),
                Value::String("2024-11-01 00:00:00".to_string()),
            ],
        );

        assert_eq!(parse_query(query), Ok(Query::Insert(expected_result)));
    }
    
    #[test]
    fn test_parse_update_request() {
        let query = "update user_sessions \
        set \
        type = 'LAPTOP', \
        timestamp = '2024-11-08 00:00:00' \
        where user_id = 12345 \
        and session_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f'";
        
        let expected_result = UpdateQuery::new(
            "user_sessions".to_string(),
            vec![
                ("type".to_string(), Value::String("LAPTOP".to_string())),
                ("timestamp".to_string(), Value::String("2024-11-08 00:00:00".to_string())),
            ],
            vec![
                Condition::new("user_id".to_string(), Operator::Equals, Value::Integer(12345)),
                Condition::new("session_id".to_string(), Operator::Equals, Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string())),
            ]
        );

        assert_eq!(parse_query(query), Ok(Query::Update(expected_result)));
    }
    
    #[test]
    fn test_parse_row_delete_request() {
        let query = "delete \
        from user_sessions \
        where user_id = 12345 \
        and session_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f'";
        
        let expected_result = DeleteQuery::new(
            vec![],
            "user_sessions".to_string(),
            vec![
                Condition::new("user_id".to_string(), Operator::Equals, Value::Integer(12345)),
                Condition::new("session_id".to_string(), Operator::Equals, Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string())),
            ]
        );

        assert_eq!(parse_query(query), Ok(Query::Delete(expected_result)));
    }

    #[test]
    fn test_parse_columns_delete_request() {
        let query = "delete type, timestamp \
        from user_sessions \
        where user_id = 12345 \
        and session_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f'";

        let expected_result = DeleteQuery::new(
            vec![
                "type".to_string(),
                "timestamp".to_string(),
            ],
            "user_sessions".to_string(),
            vec![
                Condition::new("user_id".to_string(), Operator::Equals, Value::Integer(12345)),
                Condition::new("session_id".to_string(), Operator::Equals, Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string())),
            ]
        );

        assert_eq!(parse_query(query), Ok(Query::Delete(expected_result)));
    }
}