use nom::IResult;
use nom::sequence::{delimited, tuple};
use nom::character::complete::{digit1, i64 as parse_i64, multispace0};
use nom::branch::alt;
use nom::combinator::{map, map_res, opt, recognize};
use nom::bytes::complete::{tag, tag_no_case, take_while1};
use crate::query_parser::keyword::{FALSE, TRUE};
use crate::query_parser::query::Value;

pub(crate) fn parse_keyword<'a>(keyword: &'a str) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str> {
    ws(tag_no_case(keyword))
}

pub(crate) fn parse_value(input: &str) -> IResult<&str, Value> {
    alt((
        parse_float,
        parse_integer,
        map(ws(tag_no_case(FALSE)), |_| Value::Bool(false)),
        map(ws(tag_no_case(TRUE)), |_| Value::Bool(true)),
        parse_string
    ))(input)
}

pub(crate) fn parse_string(input: &str) -> IResult<&str, Value> {
    let string_parser = ws(delimited(tag("'"), take_while1(|ch: char| ch != '\''), tag("'")));
    map(string_parser, |string: &str| Value::String(string.to_string()))(input)
}

pub(crate) fn parse_float(input: &str) -> IResult<&str, Value> {
    ws(map_res(
        recognize(tuple((opt(tag("-")), digit1, tag("."), digit1))),
        |s: &str| { s.parse::<f64>().map(Value::Float) },
    ))(input)
}

pub(crate) fn parse_integer(input: &str) -> IResult<&str, Value> {
    ws(map(parse_i64, Value::Integer))(input)
}

pub(crate) fn parse_identifier(input: &str) -> IResult<&str, String> {
    let filter = |ch: char| -> bool {
        ch.is_alphabetic() || ch == '_'
    };
    ws(map(take_while1(filter), String::from))(input)
}

pub (crate) fn parse_comma(input: &str) -> IResult<&str, &str> {
    ws(tag(","))(input)
}

pub(crate) fn ws<'a, F: 'a, O>(f: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O>,
{
    delimited(multispace0, f, multispace0)
}