use std::{net::SocketAddr, time::Duration};

use tonic::{transport::Channel, Request};

use crate::preclude::*;

pub struct KvRaftClient {
    name: String,
    seq: u64,
    num_servers: usize,
    servers: Vec<KvRpcClient<Channel>>,
    retries: usize,
    timeout: Duration,
}

impl KvRaftClient {
    pub fn builder() -> KvRaftClientBuilder {
        KvRaftClientBuilder::default()
    }
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
            name: String::from("kvs"),
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
            name: String::from("kvs"),
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
            name: String::from("kvs"),
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
                    Ok(Err(_e)) => continue,
                    Err(_e) => continue,
                }
            }
        }
        Err(KvError::Unknown)
    }
}

pub struct KvRaftClientBuilder {
    name: String,
    seq: u64,
    info: Vec<SocketAddr>,
    retries: usize,
    timeout: Duration,
}

impl Default for KvRaftClientBuilder {
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

impl KvRaftClientBuilder {
    pub fn add_node(mut self, addr: SocketAddr) -> KvRaftClientBuilder {
        self.info.push(addr);
        self
    }
    pub fn add_batch_node(mut self, mut addrs: Vec<SocketAddr>) -> KvRaftClientBuilder {
        self.info.append(&mut addrs);
        self
    }
    pub fn set_seq(mut self, seq: u64) -> KvRaftClientBuilder {
        self.seq = seq;
        self
    }
    pub fn set_name(mut self, name: String) -> KvRaftClientBuilder {
        self.name = name;
        self
    }
    pub fn set_retries(mut self, retries: usize) -> KvRaftClientBuilder {
        self.retries = retries;
        self
    }
    pub fn set_timeout(mut self, timeout: Duration) -> KvRaftClientBuilder {
        self.timeout = timeout;
        self
    }
    pub fn build(self) -> KvRaftClient {
        let servers: Vec<KvRpcClient<Channel>> = self
            .info
            .iter()
            .map(|node| format!("http://{}", node))
            .map(|node| Channel::from_shared(node).unwrap().connect_lazy().unwrap())
            .map(|res| KvRpcClient::new(res))
            .collect();

        KvRaftClient {
            name: self.name,
            seq: self.seq,
            num_servers: servers.len(),
            servers,
            retries: self.retries,
            timeout: self.timeout,
        }
    }
}
