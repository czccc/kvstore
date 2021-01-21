use crate::*;
use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
};

/// Kvs Cilent with an TCP stream
pub struct KvsClient {
    // stream: TcpStream,
    addr: SocketAddr,
    writes: Vec<(String, String)>,
    start_ts: u64,
}

impl KvsClient {
    /// Return a kvs client at given SocketAddr
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            // stream,
            addr,
            writes: Vec::new(),
            start_ts: 0,
        }
    }
    /// Send set command to server, and process the response.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request {
            cmd: "Set".to_string(),
            ts: 0,
            key,
            value: Some(value),
            primary: String::new(),
            commit_ts: 0,
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
            ts: 0,
            key,
            value: None,
            primary: String::new(),
            commit_ts: 0,
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
            ts: 0,
            key,
            value: None,
            primary: String::new(),
            commit_ts: 0,
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
        let stream = TcpStream::connect(self.addr).unwrap();
        let mut reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);

        let buf = serde_json::to_string(&request)?;
        writer.write(buf.as_bytes())?;
        writer.flush()?;

        let mut buf = String::new();
        reader.read_line(&mut buf)?;
        let response: Response = serde_json::from_str(&buf)?;
        Ok(response)
    }
}

impl KvsClient {
    pub fn txn_start(&mut self) -> Result<()> {
        self.start_ts = self.get_ts()?;
        info!("[Client {}] - Start txn", self.start_ts);
        Ok(())
    }

    fn get_ts(&mut self) -> Result<u64> {
        let request = Request {
            cmd: "TSO".to_string(),
            ts: 0,
            key: String::new(),
            value: None,
            primary: String::new(),
            commit_ts: 0,
        };
        let response = self.send_request(request)?;
        match response.status.as_str() {
            "ok" => Ok(response.result.unwrap().parse()?),
            "err" => Err(KvError::StringError(
                response.result.unwrap_or("Unknown Error".to_owned()),
            )),
            _ => Err(KvError::StringError("Unknown Status".to_owned())),
        }
    }

    pub fn txn_get(&mut self, key: String) -> Result<String> {
        info!("[Client {}] - get key: {}", self.start_ts, key);
        // unimplemented!()
        Ok(String::from("Placehold"))
    }
    pub fn txn_set(&mut self, key: String, value: String) -> Result<()> {
        info!(
            "[Client {}] - set key: {} value: {}",
            self.start_ts, key, value
        );
        self.writes.push((key, value));
        Ok(())
    }
    pub fn txn_commit(&mut self) -> Result<bool> {
        if self.writes.is_empty() {
            return Ok(true);
        }
        info!(
            "[Client {}] - Commit start with total {} writes",
            self.start_ts,
            self.writes.len()
        );
        let (primary, primary_value) = self.writes.first().unwrap().to_owned();
        for (k, v) in self.writes.clone().iter() {
            let request = Request {
                cmd: "PreWrite".to_string(),
                ts: self.start_ts,
                key: k.to_string(),
                value: Some(v.to_string()),
                primary: primary.to_string(),
                commit_ts: 0,
            };
            let response = self.send_request(request)?;
            match response.status.as_str() {
                "ok" => {
                    info!(
                        "[Client {}] - Prewrite Ok - key: {}, value: {:?}, primary: {}",
                        self.start_ts,
                        k.to_string(),
                        v.to_string(),
                        primary.to_string()
                    );
                }
                "err" => {
                    return Err(KvError::StringError(
                        response.result.unwrap_or("Unknown Error".to_owned()),
                    ))
                }
                _ => return Err(KvError::StringError("Unknown Status".to_owned())),
            };
        }

        let commit_ts = self.get_ts()?;
        let request = Request {
            cmd: "Commit".to_string(),
            ts: self.start_ts,
            key: primary.to_string(),
            value: Some(primary_value.to_string()),
            primary: primary.to_string(),
            commit_ts,
        };
        let response = self.send_request(request)?;
        match response.status.as_str() {
            "ok" => {
                info!(
                    "[Client {}] - Commit Primary TS: {} Ok - key: {}, value: {:?}, primary: {}",
                    self.start_ts,
                    commit_ts,
                    primary.to_string(),
                    primary_value.to_string(),
                    primary.to_string()
                );
            }
            "err" => {
                return Err(KvError::StringError(
                    response.result.unwrap_or("Unknown Error".to_owned()),
                ))
            }
            _ => return Err(KvError::StringError("Unknown Status".to_owned())),
        };
        for (k, v) in self.writes.clone().iter().skip(1) {
            let request = Request {
                cmd: "Commit".to_string(),
                ts: self.start_ts,
                key: k.to_string(),
                value: Some(v.to_string()),
                primary: primary.to_string(),
                commit_ts,
            };
            info!(
                "[Client {}] - Commit TS: {} - key: {}, value: {:?}, primary: {}",
                self.start_ts, commit_ts, request.key, request.value, request.primary
            );
            let response = self.send_request(request)?;
            match response.status.as_str() {
                "ok" => {}
                _ => {}
            };
        }
        Ok(true)
    }
}
