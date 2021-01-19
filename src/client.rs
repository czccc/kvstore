use crate::*;
use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
    str::from_utf8,
};

/// Kvs Cilent with an TCP stream
pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    /// Return a kvs client at given SocketAddr
    pub fn new(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        Self { stream }
    }
    /// Send set command to server, and process the response.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request {
            cmd: "Set".to_string(),
            key,
            value: Some(value),
        };
        let response = self.send_request(request)?;
        match response.status.as_str() {
            "ok" => Ok(()),
            "err" => Err(KvError::StringError(
                response.result.unwrap_or("Unknown Error".to_owned()),
            )),
            _ => Err(KvError::StringError("Unknown Status".to_owned())),
        }
    }
    /// Send get command to server, and process the response.
    pub fn get(&mut self, key: String) -> Result<String> {
        let request = Request {
            cmd: "Get".to_string(),
            key,
            value: None,
        };
        let response = self.send_request(request)?;
        match response.status.as_str() {
            "ok" => Ok(response.result.unwrap()),
            "err" => Err(KvError::StringError(
                response.result.unwrap_or("Unknown Error".to_owned()),
            )),
            _ => Err(KvError::StringError("Unknown Status".to_owned())),
        }
    }
    /// Send remove command to server, and process the response.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let request = Request {
            cmd: "Remove".to_string(),
            key,
            value: None,
        };
        let response = self.send_request(request)?;
        match response.status.as_str() {
            "ok" => Ok(()),
            "err" => Err(KvError::StringError(
                response.result.unwrap_or("Unknown Error".to_owned()),
            )),
            _ => Err(KvError::StringError("Unknown Status".to_owned())),
        }
    }
    fn send_request(&mut self, request: Request) -> Result<Response> {
        let mut reader = BufReader::new(&self.stream);
        let mut writer = BufWriter::new(&self.stream);

        let buf = serde_json::to_string(&request)?;
        writer.write(buf.as_bytes())?;
        writer.flush()?;

        let mut buf = vec![];
        reader.read_until(b'}', &mut buf)?;
        let response: Response = serde_json::from_str(from_utf8(&buf).unwrap())?;
        Ok(response)
    }
}
