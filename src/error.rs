use std::io;

use thiserror::Error;
use tonic::Status;

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
    /// RPC Error
    #[error("{0}")]
    Rpc(#[from] tonic::Status),
    /// Unknown Engine
    #[error("Unknown Engine: {0}")]
    ParserError(String),
    /// Unknown Engine
    #[error("{0}")]
    StringError(String),
    /// Unknown Error
    #[error("Not Leader")]
    NotLeader,
    /// Unknown Error
    #[error("Unknown Error")]
    Unknown,
}

/// KvStore Error Result
pub type Result<T> = std::result::Result<T, KvError>;

impl From<KvError> for Status {
    fn from(err: KvError) -> Self {
        match err {
            KvError::KeyNotFound => Status::not_found("Key not found"),
            KvError::Io(e) => Status::internal(e.to_string()),
            KvError::Serde(e) => Status::internal(e.to_string()),
            KvError::Rpc(e) => e,
            KvError::ParserError(e) => Status::internal(e.to_string()),
            KvError::StringError(e) => Status::internal(e.to_string()),
            KvError::NotLeader => Status::permission_denied("Not Leader"),
            KvError::Unknown => Status::unknown("Unknown Error"),
        }
    }
}

/// KvRpcError
#[derive(Error, Debug)]
pub enum KvRpcError {
    /// Key not found
    #[error("Key not found")]
    KeyNotFound,
    /// Unknown Error
    #[error("Not Leader")]
    NotLeader,
    /// Unknown Error
    #[error("Duplicated Request")]
    DuplicatedRequest,
    /// Unknown Error
    #[error("Timeout")]
    Timeout,
    /// Unknown Error
    #[error("Recv Error")]
    Recv,
    /// Unknown Error
    #[error("Abort: {0}")]
    Abort(String),
    /// Unknown Error
    #[error("Error: {0}")]
    Unknown(String),
}

impl From<KvRpcError> for Status {
    fn from(err: KvRpcError) -> Self {
        match err {
            KvRpcError::KeyNotFound => Status::not_found("Key not found"),
            KvRpcError::NotLeader => Status::permission_denied("Not Leader"),
            KvRpcError::DuplicatedRequest => Status::already_exists("Duplicated Request"),
            KvRpcError::Timeout => Status::deadline_exceeded("Timeout"),
            KvRpcError::Recv => Status::cancelled("Recv Error"),
            KvRpcError::Abort(e) => Status::aborted(e),
            KvRpcError::Unknown(e) => Status::unknown(e),
        }
    }
}
