use serde::{Deserialize, Serialize};
/// Protocol for client-server request
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    /// Command
    pub cmd: String,
    /// Key
    pub key: String,
    /// Value
    pub value: Option<String>,
}

/// Protocol for client-server response
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    /// Status
    pub status: String,
    /// Result
    pub result: Option<String>,
}
