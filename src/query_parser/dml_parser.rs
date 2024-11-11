use crate::query_parser::builder::{ConditionBuilder, DeleteQueryBuilder, InsertQueryBuilder, SelectQueryBuilder};
use crate::query_parser::common_parser;
use crate::query_parser::common_parser::parse_value;
use crate::query_parser::keyword::*;
use crate::query_parser::query::{Condition, DataManipulationQuery, Operator, Query, QueryParsingError, UpdateQuery, Value};
use common_parser::ws;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::map;
use nom::multi::separated_list1;
use nom::sequence::{delimited, tuple};
use nom::IResult;

pub(crate) fn parse_select_query(query: &str) -> Result<Query, QueryParsingError> {
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

    Ok(SelectQueryBuilder::new()
        .columns(columns)
        .table(table)
        .conditions(conditions)
        .build())
}

fn parse_conditions(query: &str) -> IResult<&str, Vec<Condition>> {
    match common_parser::parse_keyword(WHERE)(query) {
        Ok((query, _)) => separated_list1(common_parser::parse_keyword(AND), parse_condition)(query),
        Err(_) => Ok((query, Vec::new()))
    }
}

pub(crate) fn parse_condition(query: &str) -> IResult<&str, Condition> {
    let (query, column) = common_parser::parse_identifier(query)?;

    let (query, operator) = alt((
        map(ws(tag(GREATER_OR_EQUALS)), |_| Operator::GreaterOrEquals),
        map(ws(tag(LESS_OR_EQUALS)), |_| Operator::LessOrEquals),
        map(ws(tag(GREATER)), |_| Operator::Greater),
        map(ws(tag(LESS)), |_| Operator::Less),
        map(ws(tag(EQUALS)), |_| Operator::Equals),
        map(ws(tag(NOT_EQUALS)), |_| Operator::NotEquals),
    ))(query)?;

    let (query, value) = parse_value(query)?;

    let condition = ConditionBuilder::new()
        .column(column)
        .operator(operator)
        .value(value)
        .build();

    Ok((query, condition))
}

pub(crate) fn parse_insert(query: &str) -> Result<Query, QueryParsingError> {
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

    Ok(InsertQueryBuilder::new()
        .columns(columns)
        .table(table)
        .values(values)
        .build())
}

pub(crate) fn parse_update(query: &str) -> Result<Query, QueryParsingError> {
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

pub(crate) fn parse_delete(query: &str) -> Result<Query, QueryParsingError> {
    let query = match common_parser::parse_keyword(DELETE)(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("an error occurred while parsing delete keyword".to_string(), query.to_string()))
    };

    let (query, columns) = match common_parser::parse_keyword(FROM)(query) {
        Ok((query, _)) => (query, Vec::new()),
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

    Ok(DeleteQueryBuilder::new()
        .columns(columns)
        .table(table)
        .conditions(conditions)
        .build())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::query_parser::builder::UpdateQueryBuilder;
    use crate::query_parser::parser::parse_query;

    #[test]
    fn test_parse_select() {
        let params = vec![
            (
                r#"
                SELECT *
                FROM user_sessions
                "#,
                SelectQueryBuilder::new()
                    .table("user_sessions".to_string())
                    .build()
            ),
            (
                r#"
                SELECT user_id, session_id, type, device_type, timestamp
                FROM user_sessions
                "#,
                SelectQueryBuilder::new()
                    .column("user_id".to_string())
                    .column("session_id".to_string())
                    .column("type".to_string())
                    .column("device_type".to_string())
                    .column("timestamp".to_string())
                    .table("user_sessions".to_string())
                    .build()
            ),
            (
                r#"
                SELECT user_id, session_id, type, device_type, timestamp
                FROM user_sessions
                WHERE user_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f'
                AND timestamp >= '2024-10-21 00:00:00'
                AND timestamp <= '2024-11-01 00:00:00'
                "#,
                SelectQueryBuilder::new()
                    .column("user_id".to_string())
                    .column("session_id".to_string())
                    .column("type".to_string())
                    .column("device_type".to_string())
                    .column("timestamp".to_string())
                    .table("user_sessions".to_string())
                    .condition(ConditionBuilder::new()
                        .column("user_id".to_string())
                        .operator(Operator::Equals)
                        .value(Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string()))
                        .build())
                    .condition(ConditionBuilder::new()
                        .column("timestamp".to_string())
                        .operator(Operator::GreaterOrEquals)
                        .value(Value::String("2024-10-21 00:00:00".to_string()))
                        .build())
                    .condition(ConditionBuilder::new()
                        .column("timestamp".to_string())
                        .operator(Operator::LessOrEquals)
                        .value(Value::String("2024-11-01 00:00:00".to_string()))
                        .build())
                    .build()
            )
        ];

        for (query, expected_result) in params {
            assert_eq!(parse_query(query), Ok(expected_result));
        }
    }

    #[test]
    fn test_parse_insert_query() {
        let query = r#"
        INSERT INTO user_sessions (user_id, session_id, type, device_type, timestamp)
        VALUES (12345, '3e3be9fb-5888-4b0e-8f22-287b7d90a32f', 'LOG_IN', 'PHONE', '2024-11-01 00:00:00')"#;

        let expected_result = InsertQueryBuilder::new()
            .column("user_id".to_string())
            .column("session_id".to_string())
            .column("type".to_string())
            .column("device_type".to_string())
            .column("timestamp".to_string())
            .table("user_sessions".to_string())
            .value(Value::Integer(12345))
            .value(Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string()))
            .value(Value::String("LOG_IN".to_string()))
            .value(Value::String("PHONE".to_string()))
            .value(Value::String("2024-11-01 00:00:00".to_string()))
            .build();

        assert_eq!(parse_query(query), Ok(expected_result));
    }

    #[test]
    fn test_parse_update_request() {
        let query = r#"
        UPDATE user_sessions
        SET
        type = 'LAPTOP',
        timestamp = '2024-11-08 00:00:00'
        WHERE user_id = 12345
        AND session_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f'"#;

        let expected_result = UpdateQueryBuilder::new()
            .table("user_sessions".to_string())
            .value(("type".to_string(), Value::String("LAPTOP".to_string())))
            .value(("timestamp".to_string(), Value::String("2024-11-08 00:00:00".to_string())))
            .condition(ConditionBuilder::new()
                .column("user_id".to_string())
                .operator(Operator::Equals)
                .value(Value::Integer(12345))
                .build())
            .condition(ConditionBuilder::new()
                .column("session_id".to_string())
                .operator(Operator::Equals)
                .value(Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string()))
                .build())
            .build();

        assert_eq!(parse_query(query), Ok(expected_result));
    }

    #[test]
    fn test_parse_delete() {
        let params = vec![
            (
                r#"
                DELETE FROM user_sessions
                WHERE user_id = 12345
                AND session_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f'
                "#,
                DeleteQueryBuilder::new()
                    .table("user_sessions".to_string())
                    .condition(ConditionBuilder::new()
                        .column("user_id".to_string())
                        .operator(Operator::Equals)
                        .value(Value::Integer(12345))
                        .build())
                    .condition(ConditionBuilder::new()
                        .column("session_id".to_string())
                        .operator(Operator::Equals)
                        .value(Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string()))
                        .build())
                    .build()
            ),
            (
                r#"
                DELETE type, timestamp
                FROM user_sessions
                WHERE user_id = 12345
                AND session_id = '3e3be9fb-5888-4b0e-8f22-287b7d90a32f'
                "#,
                DeleteQueryBuilder::new()
                    .column("type".to_string())
                    .column("timestamp".to_string())
                    .table("user_sessions".to_string())
                    .condition(ConditionBuilder::new()
                        .column("user_id".to_string())
                        .operator(Operator::Equals)
                        .value(Value::Integer(12345))
                        .build())
                    .condition(ConditionBuilder::new()
                        .column("session_id".to_string())
                        .operator(Operator::Equals)
                        .value(Value::String("3e3be9fb-5888-4b0e-8f22-287b7d90a32f".to_string()))
                        .build())
                    .build()
            )
        ];

        for (query, expected_result) in params {
            assert_eq!(parse_query(query), Ok(expected_result));
        }
    }
}