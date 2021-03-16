use std::{net::SocketAddr, time::Duration};

use tonic::{transport::Channel, Code, Request};

use crate::preclude::*;

/// A KvsClient that support communicate with KvsServer
pub struct KvsClient {
    name: String,
    seq: u64,
    num_servers: usize,
    servers: Vec<KvRpcClient<Channel>>,
    retries: usize,
    timeout: Duration,
}

impl KvsClient {
    /// return a new client which only request one server
    pub fn new(addr: SocketAddr) -> KvsClient {
        KvsClientBuilder::default().add_node(addr).build()
    }
    /// return a new Builder to set some fields
    pub fn builder() -> KvsClientBuilder {
        KvsClientBuilder::default()
    }
    /// Send a request to server and get the max seq number of this client's name
    pub async fn init(&mut self) -> Result<()> {
        let req = SeqMessage {
            name: self.name.clone(),
            seq: 0,
        };
        for _retries in 0..self.retries {
            for client in self.servers.iter_mut() {
                let res = client.init(Request::new(req.clone()));
                match tokio::time::timeout(self.timeout, res).await {
                    Ok(Ok(res)) => {
                        let res = res.into_inner();
                        // info!("{}", res.message);
                        self.seq = res.seq;
                        return Ok(());
                    }
                    Ok(Err(_e)) => continue,
                    Err(_e) => continue,
                }
            }
        }
        Err(KvError::Unknown)
    }
    /// Send set command to server, and process the response.
    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        if self.seq == 0 {
            self.init().await?;
        }
        let req = SetRequest {
            key,
            value,
            name: self.name.clone(),
            seq: 1,
        };
        for _retries in 0..self.retries {
            for client in self.servers.iter_mut() {
                let res = client.set(Request::new(req.clone()));
                match tokio::time::timeout(self.timeout, res).await {
                    Ok(Ok(res)) => {
                        let _res = res.into_inner();
                        // info!("{}", res.message);
                        return Ok(());
                    }
                    Ok(Err(_e)) => continue,
                    Err(_e) => continue,
                }
            }
        }
        Err(KvError::Unknown)
    }
    /// Send get command to server, and process the response.
    pub async fn get(&mut self, key: String) -> Result<String> {
        if self.seq == 0 {
            self.init().await?;
        }
        let req = GetRequest {
            key,
            name: self.name.clone(),
            seq: 1,
        };
        for _retries in 0..self.retries {
            for client in self.servers.iter_mut() {
                let res = client.get(Request::new(req.clone()));
                match tokio::time::timeout(self.timeout, res).await {
                    Ok(Ok(res)) => {
                        let res = res.into_inner();
                        // println!("{}", res.message);
                        return Ok(res.message);
                    }
                    Ok(Err(_e)) => continue,
                    Err(_e) => continue,
                }
            }
        }
        Err(KvError::Unknown)
    }
    /// Send remove command to server, and process the response.
    pub async fn remove(&mut self, key: String) -> Result<()> {
        if self.seq == 0 {
            self.init().await?;
        }
        let req = RemoveRequest {
            key,
            name: self.name.clone(),
            seq: 1,
        };
        for _retries in 0..self.retries {
            for client in self.servers.iter_mut() {
                let res = client.remove(Request::new(req.clone()));
                match tokio::time::timeout(self.timeout, res).await {
                    Ok(Ok(res)) => {
                        let _res = res.into_inner();
                        // println!("{}", res.message);
                        return Ok(());
                    }
                    Ok(Err(e)) if e.code() == Code::NotFound => return Err(KvError::KeyNotFound),
                    Ok(Err(_e)) => continue,
                    Err(_e) => continue,
                }
            }
        }
        Err(KvError::Unknown)
    }
}

/// A Client builder
pub struct KvsClientBuilder {
    name: String,
    seq: u64,
    info: Vec<SocketAddr>,
    retries: usize,
    timeout: Duration,
}

impl Default for KvsClientBuilder {
    fn default() -> Self {
        Self {
            name: String::from("client"),
            seq: 0,
            info: Vec::new(),
            retries: 3,
            timeout: Duration::from_secs(3),
        }
    }
}

impl KvsClientBuilder {
    /// add a new server addr
    pub fn add_node(mut self, addr: SocketAddr) -> KvsClientBuilder {
        self.info.push(addr);
        self
    }
    /// add a batch of server addr
    pub fn add_batch_node(mut self, mut addrs: Vec<SocketAddr>) -> KvsClientBuilder {
        self.info.append(&mut addrs);
        self
    }
    /// set client seq
    pub fn set_seq(mut self, seq: u64) -> KvsClientBuilder {
        self.seq = seq;
        self
    }
    /// set client name
    pub fn set_name(mut self, name: String) -> KvsClientBuilder {
        self.name = name;
        self
    }
    /// set client retries times
    pub fn set_retries(mut self, retries: usize) -> KvsClientBuilder {
        self.retries = retries;
        self
    }
    /// set rpc timeout
    pub fn set_timeout(mut self, timeout: Duration) -> KvsClientBuilder {
        self.timeout = timeout;
        self
    }
    /// build the client
    pub fn build(self) -> KvsClient {
        let servers: Vec<KvRpcClient<Channel>> = self
            .info
            .iter()
            .map(|node| format!("http://{}", node))
            .map(|node| Channel::from_shared(node).unwrap().connect_lazy().unwrap())
            .map(|res| KvRpcClient::new(res))
            .collect();

        KvsClient {
            name: self.name,
            seq: self.seq,
            num_servers: servers.len(),
            servers,
            retries: self.retries,
            timeout: self.timeout,
        }
    }
}
