use crate::query_parser::query::{Query, QueryParsingError};

pub (crate) fn parse_create_table_query(query: &str) -> Result<Query, QueryParsingError> {
    todo!()
}

pub(crate) fn parse_alter_table_query(query: &str) -> Result<Query, QueryParsingError> {
    todo!()
}

#[cfg(test)]
mod test {
    
}