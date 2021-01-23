use crate::*;
use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
};

/// Kvs Cilent with an TCP stream
pub struct KvsClient {
    addr: SocketAddr,
    writes: Vec<(String, String)>,
    start_ts: u64,
    is_started: bool,
}

impl KvsClient {
    /// Return a kvs client at given SocketAddr
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            writes: Vec::new(),
            start_ts: 0,
            is_started: false,
        }
    }
    /// Single set command used in a transaction.
    /// Return whether transaction success or not.
    pub fn set(&mut self, key: String, value: String) -> Result<bool> {
        self.txn_begin()?;
        self.txn_set(key, value);
        self.txn_commit()
    }
    /// Single get command used in a transaction
    /// Return the value of given key,
    /// or "Key not found" when key is not in KvsServer or value is empty.
    pub fn get(&mut self, key: String) -> Result<String> {
        self.txn_begin()?;
        let value = self.txn_get(key);
        self.txn_commit()?;
        value
    }
    /// Single Remove command used in a transaction.
    /// Return whether transaction success or not
    /// or an Err("Key not found") when key is not in KvsServer or value is empty.
    ///
    /// For now, remove a key is same with set a key to empty string.
    pub fn remove(&mut self, key: String) -> Result<bool> {
        let value = self.get(key.to_string())?;
        info!("prev: {}", value);
        if value == "" || value == "Key not found" {
            Err(KvError::KeyNotFound)
        } else {
            self.txn_begin()?;
            self.txn_set(key, String::new());
            self.txn_commit()
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
    /// Determined whether the txn is started or not
    pub fn txn_is_started(&mut self) -> bool {
        self.is_started
    }

    /// Start a transaction with acquire a timestamp from KvsServer
    pub fn txn_begin(&mut self) -> Result<()> {
        self.start_ts = self.get_ts()?;
        self.is_started = true;
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

    /// Get a value of given key from transaction snapshot
    pub fn txn_get(&mut self, key: String) -> Result<String> {
        info!("[Client {}] - get key: {}", self.start_ts, key);
        let request = Request {
            cmd: "Get".to_string(),
            ts: self.start_ts,
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
        // Ok(String::from("Placehold"))
    }

    /// Set a value of given key in transaction,
    /// for now, it just push this operation into a queue.
    pub fn txn_set(&mut self, key: String, value: String) {
        info!(
            "[Client {}] - set key: {} value: {}",
            self.start_ts, key, value
        );
        self.writes.push((key, value));
    }

    /// Commit current transaction. Use Percolator as commit method
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
                    return Ok(false);
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
                return Ok(false);
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
        self.is_started = false;
        Ok(true)
    }
}
