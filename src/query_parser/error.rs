use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::query_parser::query::QueryParsingError;

impl Display for QueryParsingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryParsingError::UnsupportedRequest(query) => write!(f, "the request {} is not supported", query),
            QueryParsingError::QuerySyntaxError(msg, query) => write!(f, "an syntax error {} occurred while parsing the request {}", msg, query)
        }
    }
}

impl Error for QueryParsingError {}