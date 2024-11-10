use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::map;
use nom::multi::separated_list1;
use nom::sequence::{delimited, tuple};
use nom::IResult;
use common_parser::ws;
use crate::query_parser::common_parser;
use crate::query_parser::keyword::*;
use crate::query_parser::query::{Condition, DataManipulationQuery, DeleteQuery, InsertQuery, Operator, Query, QueryParsingError, SelectQuery, UpdateQuery, Value};

pub (crate) fn parse_select_query(query: &str) -> Result<Query, QueryParsingError> {
    let query = match common_parser::parse_keyword(SELECT)(query) {
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
            common_parser::parse_identifier,
        )
    ))(query);

    let (query, columns) = match parsing_result {
        Ok((query, columns)) => (query, columns),
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the column names or *".to_string(), query.to_string()))
    };

    let query = match common_parser::parse_keyword(FROM)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the from keyword".to_string(), query.to_string()))
    };

    let (query, table) = match common_parser::parse_identifier(query) {
        Ok((query, table)) => (query, table),
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the table name".to_string(), query.to_string()))
    };

    let conditions = match parse_conditions(query) {
        Ok((_, conditions)) => conditions,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing where condition".to_string(), query.to_string()))
    };

    Ok(Query::DataManipulationQuery(DataManipulationQuery::Select(SelectQuery::new(columns, table, conditions))))
}

fn parse_conditions(query: &str) -> IResult<&str, Vec<Condition>> {
    match common_parser::parse_keyword(WHERE)(query) {
        Ok((query, _)) => separated_list1(common_parser::parse_keyword(AND), parse_condition)(query),
        Err(_) => Ok((query, Vec::new()))
    }
}

pub (crate) fn parse_condition(query: &str) -> IResult<&str, Condition> {
    let (query, column) = common_parser::parse_identifier(query)?;

    let (query, operator) = alt((
        map(ws(tag(">=")), |_| Operator::GreaterOrEquals),
        map(ws(tag("<=")), |_| Operator::LessOrEquals),
        map(ws(tag(">")), |_| Operator::Greater),
        map(ws(tag("<")), |_| Operator::Less),
        map(ws(tag("=")), |_| Operator::Equals),
        map(ws(tag("!=")), |_| Operator::NotEquals),
    ))(query)?;

    let (query, value) = alt((
        common_parser::parse_float,
        common_parser::parse_integer,
        map(ws(tag("false")), |_| Value::Bool(false)),
        map(ws(tag("true")), |_| Value::Bool(true)),
        common_parser::parse_string
    ))(query)?;

    Ok((query, Condition { column, operator, value }))
}

pub (crate) fn parse_insert(query: &str) -> Result<Query, QueryParsingError> {
    let query = match common_parser::parse_keyword(INSERT_INTO)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the insert into keyword".to_string(), query.to_string()))
    };

    let (query, table) = match common_parser::parse_identifier(query) {
        Ok((query, table)) => (query, table),
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the table name".to_string(), query.to_string()))
    };

    let parsing_result = ws(delimited(
        tag("("),
        separated_list1(ws(tag(",")), common_parser::parse_identifier),
        tag(")"),
    ))(query);

    let (query, columns) = match parsing_result {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing column names".to_string(), query.to_string()))
    };

    let query = match common_parser::parse_keyword(VALUES)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected the values keyword".to_string(), query.to_string()))
    };

    let parsing_result = ws(delimited(
        tag("("),
        separated_list1(ws(tag(",")), common_parser::parse_value),
        tag(")"),
    ))(query);

    let (_, values) = match parsing_result {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing values".to_string(), query.to_string()))
    };

    Ok(Query::DataManipulationQuery(DataManipulationQuery::Insert(InsertQuery::new(columns, table, values))))
}

pub (crate) fn parse_update(query: &str) -> Result<Query, QueryParsingError> {
    let query = match common_parser::parse_keyword(UPDATE)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing update keyword".to_string(), query.to_string()))
    };

    let (query, table) = match common_parser::parse_identifier(query) {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing the table name".to_string(), query.to_string()))
    };

    let query = match common_parser::parse_keyword(SET)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected set keyword".to_string(), query.to_string()))
    };

    let (query, values) = match separated_list1(
        ws(tag(",")),
        map(
            tuple((common_parser::parse_identifier, ws(tag("=")), common_parser::parse_value)),
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

    Ok(Query::DataManipulationQuery(DataManipulationQuery::Update(UpdateQuery::new(table, values, conditions))))
}

pub (crate) fn parse_delete(query: &str) -> Result<Query, QueryParsingError> {
    let query = match common_parser::parse_keyword(DELETE)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing delete keyword".to_string(), query.to_string()))
    };
    
    let (query, columns) = match common_parser::parse_keyword(FROM)(query) {
        Ok((query,_)) => (query, Vec::new()),
        Err(_) => match separated_list1(ws(tag(",")), common_parser::parse_identifier)(query) {
            Ok((query, columns)) => match common_parser::parse_keyword(FROM)(query) {
                Ok((query, _)) => (query, columns),
                Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing from keyword".to_string(), query.to_string()))
            },
            Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing the columns".to_string(), query.to_string()))
        }
    };

    let (query, table) = match common_parser::parse_identifier(query) {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing the table name".to_string(), query.to_string()))
    };

    let conditions = match parse_conditions(query) {
        Ok((_, conditions)) => conditions,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing where condition".to_string(), query.to_string()))
    };

    Ok(Query::DataManipulationQuery(DataManipulationQuery::Delete(DeleteQuery::new(columns, table, conditions))))
}

#[cfg(test)]
mod test {
    use crate::query_parser::parser::parse_query;
    use super::*;

    #[test]
    fn test_select_all() {
        let query = "select * from sensors";
        let expected_result = SelectQuery::new(vec![], "sensors".to_string(), vec![]);
        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Select(expected_result))));
    }

    #[test]
    fn test_select_all_formated() {
        let query = "select * \n\rfrom sensors";
        let expected_result = SelectQuery::new(vec![], "sensors".to_string(), vec![]);
        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Select(expected_result))));
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
        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Select(expected_result))));
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

        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Select(expected_result))));
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

        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Select(expected_result))));
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

        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Insert(expected_result))));
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

        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Update(expected_result))));
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

        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Delete(expected_result))));
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

        assert_eq!(parse_query(query), Ok(Query::DataManipulationQuery(DataManipulationQuery::Delete(expected_result))));
    }
}