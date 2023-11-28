//! GhalaDb's errors module.
//!
use crate::core::VlogNum;
use std::path::PathBuf;
use thiserror::Error;

/// A result type that captures the [GhalaDbError]
/// in its error channel.
pub type GhalaDbResult<T> = Result<T, GhalaDbError>;

/// A list specifying various GhalaDb errors.
#[derive(Error, Debug)]
pub enum GhalaDbError {
    /// An IO error occurred.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// Data encoding using [bincode](https://docs.rs/bincode/latest/bincode/index.html) failed.
    #[error(transparent)]
    BincodeEncodeError(#[from] bincode::error::EncodeError),
    /// Data decoding using [bincode](https://docs.rs/bincode/latest/bincode/index.html) failed.
    #[error(transparent)]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
    /// A [SystemTime error](https://doc.rust-lang.org/stable/std/time/struct.SystemTimeError.html) occurred.
    #[error(transparent)]
    SystemTimeError(#[from] std::time::SystemTimeError),
    /// The datastore path exists and is, unexpectedly, not a directory.
    #[error("Database path exists but it's not a directory: {0}")]
    DbPathNotDirectory(PathBuf),
    /// A Vlog entity was not found.
    #[error("Missing Vlog: {0}")]
    MissingVlog(VlogNum),
    /// Data compression using [snap](https://docs.rs/snap/latest/snap/) failed.
    #[error(transparent)]
    DataCompressionError(#[from] snap::Error),
}
