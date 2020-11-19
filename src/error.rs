use std::io;

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
    /// Serialization or deserialization error.
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    /// Unknown Error
    #[error("unknown data store error")]
    Unknown,
}

/// KvStore Error Result
pub type Result<T> = std::result::Result<T, KvError>;

// impl From<io::Error> for KvError {
//     fn from(err: io::Error) -> Self {
//         KvError::Io(err)
//     }
// }
