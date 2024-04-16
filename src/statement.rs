use std::ops::Deref;

use crate::{
    datatype::{DataType, ScalarValue, Schema},
    errors::Error,
    table::Table,
};

pub struct InsertStatement {
    pub values: Vec<ScalarValue>,
}

pub enum Statement {
    Insert(InsertStatement),
    Read(usize),
}

impl Statement {
    fn insert_statement(values: &str, schema: &Schema) -> Result<Self, Error> {
        let values = value_tokens(values)?;

        if schema.feilds.len() != values.len() {
            return Err(Error::ParseError);
        }

        for ((_, ty), value) in schema.feilds.iter().zip(values.iter()) {
            match (ty, value) {
                (DataType::String(_), ScalarValue::String(_)) => {}
                (DataType::Number, ScalarValue::Number(_)) => {}
                _ => return Err(Error::ParseError),
            };
        }

        Ok(Statement::Insert(InsertStatement { values }))
    }
}

pub fn prepare_statement(s: &str, table: impl Deref<Target = Table>) -> Result<Statement, Error> {
    let (command, args) = s.split_once(' ').ok_or(Error::ParseError)?;
    let statement = match command {
        "insert" => Statement::insert_statement(args, table.schema())?,
        "read" => Statement::Read(args.parse().unwrap()),
        _ => return Err(Error::UnrecognizedCommand),
    };
    Ok(statement)
}

fn value_tokens(mut s: &str) -> Result<Vec<ScalarValue>, Error> {
    let mut res = vec![];

    fn number(s: &str) -> Option<(i64, &str)> {
        let (index, _) = s
            .char_indices()
            .take_while(|(_, x)| x.is_digit(10))
            .last()?;
        let (token, remainder) = s.split_at(index + 1);
        let x: i64 = token.parse::<i64>().ok()?;
        Some((x, remainder))
    }

    fn string(s: &str) -> Option<(String, &str)> {
        if s.len() < 2 && &s[0..1] != "\"" {
            return None;
        }

        let mut iter = s.char_indices().skip(1);
        let mut index: Option<usize> = None;

        while let Some((i, char)) = iter.next() {
            if char == '\\' {
                let _ = iter.next();
                continue;
            }

            if char == '"' {
                index = Some(i);
                break;
            }
        }

        let index = index?;
        let (token, remainder) = s.split_at(index + 1);
        let token = &token[1..token.len() - 1];
        let token = token.replace("\\\\", "\\");
        let token = token.replace("\\\"", "\"");
        Some((token, remainder))
    }

    while s.len() != 0 {
        if let Some((value, rem)) = number(s)
            .map(|(x, rem)| (ScalarValue::Number(x), rem))
            .or_else(|| string(s).map(|(x, rem)| (ScalarValue::String(x), rem)))
        {
            res.push(value);
            s = rem.trim();
        } else {
            return Err(Error::ParseError);
        }
    }

    Ok(res)
}
