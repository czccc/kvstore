use std::{io, num::ParseIntError};

use thiserror::Error;

/// KvStore Error
#[derive(Error, Debug)]
pub enum KvError {
    /// Key not found
    #[error("Key not found")]
    KeyNotFound,
    /// IO Error
    #[error("{0}")]
    Io(#[from] io::Error),
    /// Parser int Error
    #[error("{0}")]
    ParseInt(#[from] ParseIntError),
    /// Serialization or deserialization error.
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    /// Unknown Engine
    #[error("Unknown Engine: {0}")]
    ParserError(String),
    /// String Error
    #[error("{0}")]
    StringError(String),
    /// Unknown Error
    #[error("Unknown Error")]
    Unknown,
}

/// KvStore Error Result
pub type Result<T> = std::result::Result<T, KvError>;
