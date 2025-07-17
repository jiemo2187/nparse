//! Redis Protocol specification
//! https://redis-doc-test.readthedocs.io/en/latest/topics/protocol/

use nom::IResult;
use nom::Parser;
use nom::branch::alt;
use nom::bytes::tag;
use nom::bytes::take;
use nom::bytes::take_until;
use nom::character::complete::crlf;
use nom::combinator::complete;
use nom::combinator::map;
use nom::sequence::delimited;
use nom::sequence::terminated;
pub const CRLF: &str = "\r\n";

/// In RESP, the type of some data depends on the first byte:
///
/// For Simple Strings the first byte of the reply is "+"
/// For Errors the first byte of the reply is "-"
/// For Integers the first byte of the reply is ":"
/// For Bulk Strings the first byte of the reply is "$"
/// For Arrays the first byte of the reply is "*"
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RespValue<'a> {
    SimpleString(&'a str),
    Error(&'a str),
    Integer(i64),
    BulkString(Option<&'a str>),
    Array(Option<Vec<RespValue<'a>>>),
}

///
///
/// Simple Strings are encoded in the following way:
/// a plus character, followed by a string that cannot contain a CR or LF character (no newlines are allowed),
/// terminated by CRLF (that is "\r\n").
pub fn simple_string(i: &str) -> IResult<&str, RespValue> {
    map(
        complete(delimited(tag("+"), take_until(CRLF), crlf)),
        RespValue::SimpleString,
    )
    .parse(i)
}

#[test]
fn test_simple_strings() {
    let i = "+OK\r\n";
    assert_eq!(simple_string(i), Ok(("", RespValue::SimpleString("OK"))));
}

/// RESP Errors
///
/// RESP has a specific data type for errors.
/// Actually errors are exactly like RESP Simple Strings,
/// but the first character is a minus '-' character instead of a plus.
/// The real difference between Simple Strings and Errors in RESP is
/// that errors are treated by clients as exceptions,
/// and the string that composes the Error type is the error message itself.
pub fn error(i: &str) -> IResult<&str, RespValue> {
    map(
        complete(delimited(tag("-"), take_until(CRLF), crlf)),
        RespValue::Error,
    )
    .parse(i)
}

#[test]
fn test_errors() {
    use nom::multi::many0;

    let i = "-Error message\r\n";
    assert_eq!(error(i), Ok(("", RespValue::Error("Error message"))));
    let i = "-ERR unknown command 'foobar'\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    assert_eq!(
        many0(error).parse(i),
        Ok((
            "",
            vec![
                RespValue::Error("ERR unknown command 'foobar'"),
                RespValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            ]
        ))
    );
}

/// RESP Integers
///
/// This type is just a CRLF terminated string representing an integer,
/// prefixed by a ":" byte.
/// For example ":0\r\n", or ":1000\r\n" are integer replies.
pub fn integer(i: &str) -> IResult<&str, RespValue> {
    use nom::character::complete::i64;

    map(complete(delimited(tag(":"), i64, crlf)), RespValue::Integer).parse(i)
}

#[test]
fn test_integer() {
    use nom::multi::many0;
    let i = ":0\r\n";
    assert_eq!(integer(i), Ok(("", RespValue::Integer(0))));
    let i = ":1000\r\n:-1000\r\n";
    assert_eq!(
        many0(integer).parse(i),
        Ok((
            "",
            vec![RespValue::Integer(1000), RespValue::Integer(-1000)]
        ))
    );
}

/// RESP Bulk Strings
///
/// Bulk Strings are used in order to represent a single binary safe string up to 512 MB in length.
///
/// Bulk Strings are encoded in the following way:
///
/// A "$" byte followed by the number of bytes composing the string (a prefixed length), terminated by CRLF.
/// The actual string data.
/// A final CRLF.
pub fn bulk_string(i: &str) -> IResult<&str, RespValue> {
    use nom::character::complete::isize;

    let (i, len) = complete(delimited(tag("$"), isize, crlf)).parse(i)?;
    if len == -1 {
        Ok((i, RespValue::BulkString(None)))
    } else {
        map(terminated(take(len as usize), crlf), |str| {
            RespValue::BulkString(Some(str))
        })
        .parse(i)
    }
}

#[test]
fn test_bulk_string() {
    let i = "$6\r\nfoobar\r\n";
    assert_eq!(
        bulk_string(i),
        Ok(("", RespValue::BulkString(Some("foobar"))))
    );
    let i = "$0\r\n\r\n";
    assert_eq!(bulk_string(i), Ok(("", RespValue::BulkString(Some("")))));
    let i = "$-1\r\n";
    assert_eq!(bulk_string(i), Ok(("", RespValue::BulkString(None))));
}

/// RESP Arrays
///
/// RESP Arrays are sent using the following format:
///
/// A * character as the first byte, followed by the number of elements in the array as a decimal number,
/// followed by CRLF.
///
/// An additional RESP type for every element of the Array.
pub fn array(i: &str) -> IResult<&str, RespValue> {
    use nom::character::complete::isize;

    let (i, len) = complete(delimited(tag("*"), isize, crlf)).parse(i)?;
    if len == -1 {
        return Ok((i, RespValue::Array(None)));
    }
    let mut vec = Vec::with_capacity(len as usize);
    let mut rset = i;
    for _ in 0..len {
        let (i, o) = alt((simple_string, error, integer, bulk_string, array)).parse(rset)?;
        rset = i;
        vec.push(o);
    }
    Ok((rset, RespValue::Array(Some(vec))))
}

#[test]
fn test_array() {
    use crate::resp::RespValue::*;

    let i = "*0\r\n";
    assert_eq!(array(i), Ok(("", Array(Some(vec![])))));
    let i = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    assert_eq!(
        array(i),
        Ok((
            "",
            Array(Some(vec![BulkString(Some("foo")), BulkString(Some("bar"))]))
        ))
    );
    let i = "*3\r\n:1\r\n:2\r\n:3\r\n";
    assert_eq!(
        array(i),
        Ok(("", Array(Some(vec![Integer(1), Integer(2), Integer(3),]))))
    );
    let i = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n";
    assert_eq!(
        array(i),
        Ok((
            "",
            Array(Some(vec![
                Integer(1),
                Integer(2),
                Integer(3),
                Integer(4),
                BulkString(Some("foobar")),
            ]))
        ))
    );

    let i = "*-1\r\n";
    assert_eq!(array(i), Ok(("", Array(None))));

    let i = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
    assert_eq!(
        array(i),
        Ok((
            "",
            Array(Some(vec![
                Array(Some(vec![Integer(1), Integer(2), Integer(3),])),
                Array(Some(vec![SimpleString("Foo"), Error("Bar")])),
            ]))
        ))
    );

    // Null elements in Arrays
    let i = "*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n";
    assert_eq!(
        array(i),
        Ok((
            "",
            Array(Some(vec![
                BulkString(Some("foo")),
                BulkString(None),
                BulkString(Some("bar")),
            ]))
        ))
    );
}
