//! All fallible operations in this crate return `Result<T, LakehouseError>`.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LakehouseError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("schema validation failed: {0}")]
    SchemaValidation(String),

    #[error("cannot write an empty batch (0 rows)")]
    EmptyBatch,
}

pub type Result<T> = std::result::Result<T, LakehouseError>;
