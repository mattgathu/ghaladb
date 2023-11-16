use std::path::PathBuf;

use thiserror::Error;

use crate::core::VlogNum;

pub type GhalaDbResult<T> = Result<T, GhalaDBError>;

#[derive(Error, Debug)]
pub enum GhalaDBError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    BincodeError(#[from] bincode::Error),
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),
    #[error(transparent)]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("Failed to Send SSTable: {0}")]
    SstSendError(String),
    #[error("Database path exists but it's not a directory: {0}")]
    DbPathNotDirectory(PathBuf),
    #[error("Timed out while reading data.")]
    ReadTimeoutError,
    #[error("Compaction Error: {0}")]
    CompactionError(String),
    #[error("Failed to load sst: {0}")]
    SstLoadError(String),
    #[error("Missing Vlog: {0}")]
    MissingVlog(VlogNum),
    #[error("{0}")]
    VlogReadError(String),
    #[error("Missing value entry for key: {0:?}")]
    MissingValueEntry(Vec<u8>),
    #[error(transparent)]
    DataCompressionError(#[from] snap::Error),
}
