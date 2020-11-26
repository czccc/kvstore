use serde::{Deserialize, Serialize};
/// Request Protocol struct for client-server request
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    /// Command, which can be "get", "set", "remove".
    pub cmd: String,
    /// The specific Key in Command
    pub key: String,
    /// Value, only require when Command is "get"
    pub value: Option<String>,
}

/// Protocol struct for client-server response
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    /// Return status, which can be "ok" or "err"
    pub status: String,
    /// Result which contains the "value" or Error Message.
    pub result: Option<String>,
}
