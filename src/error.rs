use crate::core::VlogNum;
use std::path::PathBuf;
use thiserror::Error;

pub type GhalaDbResult<T> = Result<T, GhalaDBError>;

#[derive(Error, Debug)]
pub enum GhalaDBError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    BincodeEncodeError(#[from] bincode::error::EncodeError),
    #[error(transparent)]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
    #[error(transparent)]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("Database path exists but it's not a directory: {0}")]
    DbPathNotDirectory(PathBuf),
    #[error("Missing Vlog: {0}")]
    MissingVlog(VlogNum),
    #[error(transparent)]
    DataCompressionError(#[from] snap::Error),
}
