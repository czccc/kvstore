use std::u64;

use serde::{Deserialize, Serialize};
/// Request Protocol struct for client-server request
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    /// Command, which can be "Get", "Set", "Remove", "TSO", "PreWrite" and "Commit".
    pub cmd: String,
    /// TimeStamp used in txn
    pub ts: u64,
    /// The specific Key in Command
    pub key: String,
    /// Value, only require when Command is "get"
    pub value: Option<String>,
    /// Primary Key
    pub primary: String,
    /// Commit TimeStamp
    pub commit_ts: u64,
}

/// Protocol struct for client-server response
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    /// Return status, which can be "ok" or "err"
    pub status: String,
    /// Result which contains the "value" or Error Message.
    pub result: Option<String>,
}
