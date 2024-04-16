use std::{
    env::current_dir,
    ops::DerefMut,
    sync::{Mutex, OnceLock},
};

use commands::Command;
use execution::execution;
use repl::Repl;
use statement::prepare_statement;

use crate::{
    datatype::{DataType, Schema},
    table::Table,
};

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGE: usize = 100;

mod commands;
mod datatype;
mod errors;
mod execution;
mod repl;
mod statement;
mod table;

fn global_table() -> &'static Mutex<table::Table> {
    static TABLE: OnceLock<Mutex<Table>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let schema = Schema {
            feilds: vec![
                ("a".to_string(), DataType::Number),
                ("b".to_string(), DataType::String(10)),
            ],
        };

        Mutex::new(
            Table::new(
                "global".to_string(),
                schema,
                &current_dir().unwrap().join("global.db"),
            )
            .unwrap(),
        )
    })
}

fn main() -> Result<(), errors::Error> {
    let mut repl = Repl::new();
    repl.init();
    while let Some(line) = repl.input() {
        if line.chars().nth(0) == Some('.') {
            let cmd: Command = line.parse()?;
            commands::do_meta_commands(cmd)?;
        }

        let mut table = global_table().lock().unwrap();
        let statement = prepare_statement(&line, &*table)?;

        execution(statement, table.deref_mut()).unwrap();
    }
    Ok(())
}
