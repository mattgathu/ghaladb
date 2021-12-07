use std::path::PathBuf;

use thiserror::Error;

use crate::ssm::SsmCmd;

pub type GhalaDbResult<T> = Result<T, GhalaDBError>;

#[derive(Error, Debug)]
pub enum GhalaDBError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    SerializationError(#[from] bincode::Error),
    #[error(transparent)]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("Database path exists but it's not a directory: {0}")]
    DbPathNotDirectory(PathBuf),
    #[error("Failed to send storage manager command: {0:?}")]
    SsmCmdSendError(SsmCmd),
}
