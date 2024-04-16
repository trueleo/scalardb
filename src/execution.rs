use crate::errors::Error;
use crate::statement::Statement;
use crate::table::Table;

pub fn execution(statement: Statement, table: &mut Table) -> Result<(), Error> {
    match statement {
        Statement::Insert(insert_statement) => table.insert(insert_statement),
        Statement::Read(index) => table.read(index),
    }
}
