use nom::IResult;
use nom::branch::alt;
use nom::combinator::map;
use crate::query_parser::{common_parser, ddl_parser, dml_parser};
use crate::query_parser::keyword::*;
use crate::query_parser::query::{Query, QueryParsingError, QueryType};

pub(crate) fn parse_query(query: &str) -> Result<Query, QueryParsingError> {
    let query_type = get_query_type(query)?;

    match query_type {
        QueryType::Select => dml_parser::parse_select_query(query),
        QueryType::Insert => dml_parser::parse_insert(query),
        QueryType::Update => dml_parser::parse_update(query),
        QueryType::Delete => dml_parser::parse_delete(query),
        QueryType::CreateTable => ddl_parser::parse_create_table_query(query),
        QueryType::AlterTable => ddl_parser::parse_alter_table_query(query)
    }
}

fn get_query_type(query: &str) -> Result<QueryType, QueryParsingError> {
    let query_type_result: IResult<&str, QueryType> = alt((
        map(common_parser::parse_keyword(SELECT), |_| QueryType::Select),
        map(common_parser::parse_keyword(INSERT_INTO), |_| QueryType::Insert),
        map(common_parser::parse_keyword(UPDATE), |_| QueryType::Update),
        map(common_parser::parse_keyword(DELETE), |_| QueryType::Delete),
        map(common_parser::parse_keyword(CREATE_TABLE), |_| QueryType::Delete),
        map(common_parser::parse_keyword(ALTER_TABLE), |_| QueryType::Delete),
    ))(query);

    match query_type_result {
        Ok((_, query_type)) => Ok(query_type),
        Err(_) => Err(QueryParsingError::UnsupportedRequest(query.to_string()))
    }
}