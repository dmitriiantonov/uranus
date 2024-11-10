use crate::query_parser::common_parser::{parse_comma, parse_identifier, parse_keyword, ws};
use crate::query_parser::keyword::{ADD, ALTER_TABLE, BOOL, CREATE_TABLE, DOUBLE, DROP, DROP_TABLE, FLOAT, INT, LONG, PRIMARY_KEY, TEXT, TIMESTAMP, UUID};
use crate::query_parser::query::{AddColumnCondition, AlterTableCondition, AlterTableQuery, Column, ColumnType, CreateTableQuery, DataDefinitionQuery, DropColumnCondition, DropTableQuery, PrimaryKey, Query, QueryParsingError};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::{separated_list0, separated_list1};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;

pub(crate) fn parse_create_table_query(query: &str) -> Result<Query, QueryParsingError> {
    let query = match ws(parse_keyword(CREATE_TABLE))(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("cannot parse statement 'CREATE TABLE'".to_string(), query.to_string()))
    };

    let (query, table) = match parse_identifier(query) {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("cannot parse table name".to_string(), query.to_string()))
    };

    let (columns, primary_key) = if is_single_pk(query) {
        match delimited(ws(tag("(")), parse_create_table_with_single_pk, ws(tag(")")))(query) {
            Ok((_, (columns, primary_key))) => (columns, primary_key),
            Err(_) => return Err(QueryParsingError::QuerySyntaxError("cannot parse the column definition with a simple primary key".to_string(), query.to_string()))
        }
    } else {
        match delimited(ws(tag("(")), parse_create_table_with_composite_pk, ws(tag(")")))(query) {
            Ok((_, (columns, primary_key))) => (columns, primary_key),
            Err(_) => return Err(QueryParsingError::QuerySyntaxError("cannot parse the column definition with a composite primary key".to_string(), query.to_string()))
        }
    };

    Ok(Query::DataDefinitionQuery(DataDefinitionQuery::CreateTable(CreateTableQuery {
        table,
        primary_key,
        columns,
    })))
}

pub(crate) fn parse_alter_table_query(query: &str) -> Result<Query, QueryParsingError> {
    let query = match ws(parse_keyword(ALTER_TABLE))(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected 'DROP TABLE' statement".to_string(), query.to_string()))
    };

    let (query, table) = match parse_identifier(query) {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("cannot parse table name".to_string(), query.to_string()))
    };

    let conditions = match parse_alter_table_condition(query) {
        Ok((_, conditions)) => conditions,
        Err(_) => todo!()
    };

    Ok(Query::DataDefinitionQuery(DataDefinitionQuery::AlterTable(AlterTableQuery { table, conditions })))
}

pub(crate) fn parse_drop_table_query(query: &str) -> Result<Query, QueryParsingError> {
    let query = match ws(parse_keyword(DROP_TABLE))(query) {
        Ok((query, _)) => query,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("expected 'DROP TABLE' statement".to_string(), query.to_string()))
    };

    let (_, table) = match parse_identifier(query) {
        Ok(result) => result,
        Err(_) => return Err(QueryParsingError::QuerySyntaxError("cannot parse table name".to_string(), query.to_string()))
    };

    Ok(Query::DataDefinitionQuery(DataDefinitionQuery::DropTable(DropTableQuery { table })))
}

fn is_single_pk(query: &str) -> bool {
    tuple((tag("("), parse_identifier, parse_column_type, ws(tag(PRIMARY_KEY))))(query).is_ok()
}

fn parse_create_table_with_single_pk(query: &str) -> IResult<&str, (Vec<Column>, PrimaryKey)> {
    let (query, (first_column, primary_key)) = map(
        tuple((parse_identifier, parse_column_type, parse_keyword(PRIMARY_KEY), opt(parse_comma))),
        |(column_name, column_type, _, _)| {
            let column = Column {
                name: column_name.clone(),
                column_type,
            };

            let primary_key = PrimaryKey {
                partition_key: vec![column_name],
                clustering_key: vec![],
            };

            (column, primary_key)
        },
    )(query)?;

    let (query, mut other_columns) = separated_list0(
        parse_comma,
        map(
            tuple((parse_identifier, parse_column_type)),
            |(column_name, column_type)| Column { name: column_name, column_type },
        ),
    )(query)?;

    let mut columns = Vec::new();
    columns.push(first_column);
    columns.append(&mut other_columns);

    Ok((query, (columns, primary_key)))
}

fn parse_create_table_with_composite_pk(query: &str) -> IResult<&str, (Vec<Column>, PrimaryKey)> {
    let (query, columns) = terminated(
        separated_list1(
            parse_comma,
            map(
                tuple((parse_identifier, parse_column_type)),
                |(column_name, column_type)| Column { name: column_name, column_type },
            ),
        ),
        ws(tag(",")),
    )(query)?;

    let (query, _) = parse_keyword(PRIMARY_KEY)(query)?;
    let (query, primary_key) = parse_composite_pk(query)?;
    Ok((query, (columns, primary_key)))
}

fn parse_composite_pk(query: &str) -> IResult<&str, PrimaryKey> {
    delimited(
        ws(tag("(")),
        alt((
            map(tuple((
                delimited(ws(tag("(")), separated_list1(parse_comma, map(parse_identifier, |column_name| column_name)), ws(tag(")"))),
                preceded(ws(tag(",")), separated_list0(parse_comma, map(parse_identifier, |column_name| column_name))))
            ),
                |(partition_key, clustering_key), | PrimaryKey { partition_key, clustering_key },
            ),
            map(
                separated_list1(parse_comma, map(parse_identifier, |column_name| column_name)),
                |mut column_names| {
                    let partition_key = column_names.remove(0);
                    PrimaryKey {
                        partition_key: vec![partition_key],
                        clustering_key: column_names,
                    }
                },
            )
        )),
        ws(tag(")")),
    )(query)
}

fn parse_alter_table_condition(query: &str) -> IResult<&str, Vec<AlterTableCondition>> {
    map(
        separated_list0(ws(tag(",")), alt((parse_add_column, parse_drop_column))),
        |conditions| conditions.into_iter().flatten().collect(),
    )(query)
}

fn parse_add_column(query: &str) -> IResult<&str, Vec<AlterTableCondition>> {
    let single_add_parser = map(
        tuple((parse_identifier, parse_column_type)), |(column_name, column_type)| {
            vec![AlterTableCondition::AddColumn(AddColumnCondition { column_name, column_type })]
        });

    let multi_add_parser = delimited(
        ws(tag("(")),
        separated_list1(
            ws(tag(",")),
            map(
                tuple((parse_identifier, parse_column_type)),
                |(column_name, column_type)|
                    AlterTableCondition::AddColumn(AddColumnCondition { column_name, column_type }),
            ),
        ),
        ws(tag(")")),
    );

    preceded(ws(tag_no_case(ADD)), alt((single_add_parser, multi_add_parser)))(query)
}

fn parse_drop_column(query: &str) -> IResult<&str, Vec<AlterTableCondition>> {
    let single_delete_parser = map(
        parse_identifier,
        |column_name| vec![AlterTableCondition::DropColumn(DropColumnCondition { column_name })],
    );

    let multi_delete_parser = delimited(
        ws(tag("(")),
        separated_list1(
            ws(tag(",")),
            map(parse_identifier, |column_name| AlterTableCondition::DropColumn(DropColumnCondition { column_name })),
        ),
        ws(tag(")")),
    );

    preceded(ws(tag_no_case(DROP)), alt((single_delete_parser, multi_delete_parser)))(query)
}

fn parse_column_type(query: &str) -> IResult<&str, ColumnType> {
    alt((
        map(parse_keyword(UUID), |_| ColumnType::Uuid),
        map(parse_keyword(INT), |_| ColumnType::Int),
        map(parse_keyword(LONG), |_| ColumnType::Long),
        map(parse_keyword(FLOAT), |_| ColumnType::Float),
        map(parse_keyword(DOUBLE), |_| ColumnType::Double),
        map(parse_keyword(TIMESTAMP), |_| ColumnType::Timestamp),
        map(parse_keyword(TEXT), |_| ColumnType::Text),
        map(parse_keyword(BOOL), |_| ColumnType::Bool),
    ))(query)
}

#[cfg(test)]
mod test {
    use crate::query_parser::parser::parse_query;
    use super::*;

    #[test]
    fn test_parse_create_table_query() {
        let params = vec![
            (
                "CREATE TABLE products (title TEXT PRIMARY KEY, price DOUBLE, quantity INT)",
                CreateTableQuery {
                    table: "products".to_string(),
                    primary_key: PrimaryKey {
                        partition_key: vec!["title".to_string()],
                        clustering_key: vec![]
                    },
                    columns: vec![
                        Column {
                            name: "title".to_string(),
                            column_type: ColumnType::Text,
                        },
                        Column {
                            name: "price".to_string(),
                            column_type: ColumnType::Double,
                        },
                        Column {
                            name: "quantity".to_string(),
                            column_type: ColumnType::Int,
                        }
                    ],
                }
            ),
            (
                "CREATE TABLE products (title TEXT, price DOUBLE, quantity INT, PRIMARY KEY (title))",
                CreateTableQuery {
                    table: "products".to_string(),
                    primary_key: PrimaryKey {
                        partition_key: vec!["title".to_string()],
                        clustering_key: vec![]
                    },
                    columns: vec![
                        Column {
                            name: "title".to_string(),
                            column_type: ColumnType::Text,
                        },
                        Column {
                            name: "price".to_string(),
                            column_type: ColumnType::Double,
                        },
                        Column {
                            name: "quantity".to_string(),
                            column_type: ColumnType::Int,
                        }
                    ],
                }
            ),
            (
                "CREATE TABLE user_sessions (user_id UUID, session_id UUID, timestamp TIMESTAMP, device_type TEXT, PRIMARY KEY ((user_id, session_id), timestamp))",
                CreateTableQuery {
                    table: "user_sessions".to_string(),
                    primary_key: PrimaryKey {
                        partition_key: vec![
                            "user_id".to_string(),
                            "session_id".to_string(),
                        ],
                        clustering_key: vec![
                            "timestamp".to_string()
                        ]
                    },
                    columns: vec![
                        Column {
                            name: "user_id".to_string(),
                            column_type: ColumnType::Uuid,
                        },
                        Column {
                            name: "session_id".to_string(),
                            column_type: ColumnType::Uuid,
                        },
                        Column {
                            name: "timestamp".to_string(),
                            column_type: ColumnType::Timestamp,
                        },
                        Column {
                            name: "device_type".to_string(),
                            column_type: ColumnType::Text,
                        },
                    ],
                }
            ),
            (
                "CREATE TABLE posts (user_id UUID, blog_id UUID, post_id UUID, created_at TIMESTAMP, content TEXT, seen LONG, PRIMARY KEY (user_id, blog_id, post_id))",
                CreateTableQuery {
                    table: "posts".to_string(),
                    primary_key: PrimaryKey {
                        partition_key: vec![
                            "user_id".to_string()
                        ],
                        clustering_key: vec![
                            "blog_id".to_string(),
                            "post_id".to_string(),
                        ]
                    },
                    columns: vec![
                        Column {
                            name: "user_id".to_string(),
                            column_type: ColumnType::Uuid,
                        },
                        Column {
                            name: "blog_id".to_string(),
                            column_type: ColumnType::Uuid,
                        },
                        Column {
                            name: "post_id".to_string(),
                            column_type: ColumnType::Uuid,
                        },
                        Column {
                            name: "created_at".to_string(),
                            column_type: ColumnType::Timestamp,
                        },
                        Column {
                            name: "content".to_string(),
                            column_type: ColumnType::Text,
                        },
                        Column {
                            name: "seen".to_string(),
                            column_type: ColumnType::Long,
                        },
                    ],
                })
        ];

        for (query, expected_result) in params {
            assert_eq!(parse_query(query), Ok(Query::DataDefinitionQuery(DataDefinitionQuery::CreateTable(expected_result))));
        }
    }

    #[test]
    fn test_alter_table() {
        let params = vec![
            (
                "ALTER TABLE products ADD description TEXT",
                AlterTableQuery {
                    table: "products".to_string(),
                    conditions: vec![
                        AlterTableCondition::AddColumn(AddColumnCondition {
                            column_name: "description".to_string(),
                            column_type: ColumnType::Text,
                        })
                    ]
                }
            ),
            (
                "ALTER TABLE products ADD (description TEXT, price DOUBLE)",
                AlterTableQuery {
                    table: "products".to_string(),
                    conditions: vec![
                        AlterTableCondition::AddColumn(AddColumnCondition {
                            column_name: "description".to_string(),
                            column_type: ColumnType::Text,
                        }),
                        AlterTableCondition::AddColumn(AddColumnCondition {
                            column_name: "price".to_string(),
                            column_type: ColumnType::Double,
                        })
                    ]
                }
            ),
            (
                "ALTER TABLE products DROP description",
                AlterTableQuery {
                    table: "products".to_string(),
                    conditions: vec![
                        AlterTableCondition::DropColumn(DropColumnCondition {
                            column_name: "description".to_string()
                        })
                    ]
                }
            ),
            (
                "ALTER TABLE products DROP (description, price)",
                AlterTableQuery {
                    table: "products".to_string(),
                    conditions: vec![
                        AlterTableCondition::DropColumn(DropColumnCondition {
                            column_name: "description".to_string(),
                        }),
                        AlterTableCondition::DropColumn(DropColumnCondition {
                            column_name: "price".to_string(),
                        })
                    ]
                }
            ),
            (
                "ALTER TABLE products ADD description TEXT, DROP crated_at",
                AlterTableQuery {
                    table: "products".to_string(),
                    conditions: vec![
                        AlterTableCondition::AddColumn(AddColumnCondition {
                            column_name: "description".to_string(),
                            column_type: ColumnType::Text,
                        }),
                        AlterTableCondition::DropColumn(DropColumnCondition {
                            column_name: "crated_at".to_string(),
                        })
                    ]
                }
            )
        ];

        for (query, expected_result) in params {
            assert_eq!(parse_query(query), Ok(Query::DataDefinitionQuery(DataDefinitionQuery::AlterTable(expected_result))));
        }
    }

    #[test]
    fn test_drop_table() {
        let query = "DROP TABLE persons";
        let expected_result = DropTableQuery { table: "persons".to_string() };
        assert_eq!(parse_query(query), Ok(Query::DataDefinitionQuery(DataDefinitionQuery::DropTable(expected_result))));
    }
}