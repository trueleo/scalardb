#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unrecognized Command")]
    UnrecognizedCommand,
    #[error("Parse Error")]
    ParseError,
    #[error("Max number of rows for this table is reached")]
    RowLimit,
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),
}
