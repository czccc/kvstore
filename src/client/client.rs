use std::{net::SocketAddr, time::Duration};

use tonic::{transport::Channel, Code, Request};

use crate::preclude::*;

#[derive(Debug, Clone)]
struct WriteInfo {
    key: String,
    value: String,
    op: WriteOp,
}

/// A KvsClient that support communicate with KvsServer
pub struct KvsClient {
    name: String,
    ts: Option<u64>,
    seq: u64,
    num_servers: usize,
    servers: Vec<KvRpcClient<Channel>>,
    retries: usize,
    timeout: Duration,
    write_infos: Vec<WriteInfo>,
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
}

// impl KvsClient {
//     fn retry_rpc<M, N, F>(&self, rpc_call: F)
//     where
//         M: Message,
//         F: FnOnce(&mut M) -> impl Future<Output = Result<Response<N>, Status>>,
//     {
//     }
// }

impl KvsClient {
    /// determined whether txn is started or not
    pub fn txn_is_started(&self) -> bool {
        self.ts.is_some()
    }
    /// Send a request to server and get the max seq number of this client's name
    pub async fn get_timestamp(&mut self) -> Result<u64> {
        let req = TsRequest {
            name: self.name.clone(),
        };
        for _retries in 0..self.retries {
            for client in self.servers.iter_mut() {
                let res = client.get_timestamp(Request::new(req.clone()));
                match tokio::time::timeout(self.timeout, res).await {
                    Ok(Ok(res)) => {
                        let res = res.into_inner();
                        info!("get timestamp: {:?}", res.ts);
                        // self.ts = Some(res.ts);
                        return Ok(res.ts);
                    }
                    Ok(Err(_e)) => continue,
                    Err(_e) => continue,
                }
            }
        }
        Err(KvError::StringError(String::from("Unable to timestamp")))
    }
}

impl KvsClient {
    /// Send set command to server, and process the response.
    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        self.txn_start().await?;
        let _value = self.txn_get(key.clone()).await;
        self.txn_set(key, value)?;
        self.txn_commit().await
    }
    /// Send get command to server, and process the response.
    pub async fn get(&mut self, key: String) -> Result<String> {
        self.txn_start().await?;
        self.txn_get(key.clone()).await
    }

    /// Send remove command to server, and process the response.
    pub async fn remove(&mut self, key: String) -> Result<()> {
        self.txn_start().await?;
        let _value = self.txn_get(key.clone()).await?;
        self.txn_delete(key)?;
        self.txn_commit().await
    }
}

impl KvsClient {
    /// Start a transaction
    pub async fn txn_start(&mut self) -> Result<()> {
        let ts = self.get_timestamp().await.unwrap();
        self.ts = Some(ts);
        self.seq = 0;
        Ok(())
    }
    /// Set a value
    pub fn txn_set(&mut self, key: String, value: String) -> Result<()> {
        let info = WriteInfo {
            key,
            value,
            op: WriteOp::Put,
        };
        self.write_infos.push(info);
        Ok(())
    }
    /// Delete a value
    pub fn txn_delete(&mut self, key: String) -> Result<()> {
        let info = WriteInfo {
            key,
            value: String::new(),
            op: WriteOp::Delete,
        };
        self.write_infos.push(info);
        Ok(())
    }
    /// Get a value
    pub async fn txn_get(&mut self, key: String) -> Result<String> {
        self.seq += 1;
        let req = GetRequest {
            key,
            ts: self.ts.unwrap(),
            seq: self.seq,
        };
        for client in self.servers.iter_mut() {
            // let call = |req| client.txn_get(Request::new(req.clone()));
            let res = client.txn_get(Request::new(req.clone()));
            match tokio::time::timeout(self.timeout, res).await {
                Ok(Ok(res)) => {
                    let res = res.into_inner();
                    // println!("{}", res.message);
                    return Ok(res.message);
                }
                Ok(Err(e)) if e.code() == Code::PermissionDenied => {
                    continue;
                }
                Ok(Err(e)) if e.code() == Code::NotFound => {
                    return Err(KvError::KeyNotFound);
                }
                Ok(Err(e)) => return Err(KvError::StringError(e.to_string())),
                Err(e) => {
                    info!("{}", e.to_string());
                    continue;
                }
            }
        }

        Err(KvError::Unknown)
    }
    /// prewrite
    async fn txn_prewrite(&mut self, info: WriteInfo, primary: String) -> Result<()> {
        self.seq += 1;
        let req = PrewriteRequest {
            key: info.key,
            value: info.value,
            op: info.op.into(),
            primary,
            ts: self.ts.unwrap(),
            seq: self.seq,
        };
        info!(
            "try to prewrite {} : {} , primary: {}, ts: {}, seq: {}",
            req.key, req.value, req.primary, req.ts, req.seq
        );
        for _retries in 0..self.retries {
            for client in self.servers.iter_mut() {
                let res = client.txn_prewrite(Request::new(req.clone()));
                match tokio::time::timeout(self.timeout, res).await {
                    Ok(Ok(res)) => {
                        let res = res.into_inner();
                        if res.ok {
                            info!("Prewrite ok");
                            return Ok(());
                        } else {
                            return Err(KvError::Unknown);
                        }
                    }
                    Ok(Err(e)) if e.code() == Code::PermissionDenied => {
                        continue;
                    }
                    Ok(Err(e)) => return Err(KvError::StringError(e.to_string())),
                    Err(e) => {
                        info!("{}", e.to_string());
                        continue;
                    }
                }
            }
        }
        Err(KvError::Unknown)
    }
    /// Commit this transaction
    pub async fn txn_commit(&mut self) -> Result<()> {
        let primary_write = self.write_infos.first().unwrap().to_owned();
        let primary = primary_write.key.clone();
        for info in self.write_infos.clone().into_iter() {
            self.txn_prewrite(info, primary.clone()).await?;
        }
        let commit_ts = self.get_timestamp().await?;
        self.seq = 1;
        let primary_request = CommitRequest {
            is_primary: true,
            primary: primary.clone(),
            key: primary.clone(),
            op: primary_write.op.into(),
            start_ts: self.ts.unwrap(),
            commit_ts,
            seq: self.seq,
        };
        let mut ok = false;
        for client in self.servers.iter_mut() {
            let res = client.txn_commit(Request::new(primary_request.clone()));
            match tokio::time::timeout(self.timeout, res).await {
                Ok(Ok(res)) => {
                    let res = res.into_inner();
                    if res.ok {
                        ok = true;
                        break;
                    } else {
                        return Err(KvError::Unknown);
                    }
                }
                Ok(Err(e)) if e.code() == Code::PermissionDenied || e.code() == Code::DataLoss => {
                    continue;
                }
                Ok(Err(e)) => return Err(KvError::StringError(e.to_string())),
                Err(e) => {
                    info!("{}", e.to_string());
                    continue;
                }
            }
        }

        if !ok {
            return Err(KvError::Unknown);
        }
        for info in self.write_infos.clone().into_iter().skip(1) {
            self.seq += 1;
            let request = CommitRequest {
                is_primary: false,
                primary: primary.clone(),
                key: info.key.clone(),
                op: info.op.into(),
                start_ts: self.ts.unwrap(),
                commit_ts,
                seq: self.seq,
            };
            for client in self.servers.iter_mut() {
                let res = client.txn_commit(Request::new(request.clone()));
                match tokio::time::timeout(self.timeout, res).await {
                    Ok(Ok(_)) => {
                        break;
                    }
                    Ok(Err(_e)) => continue,
                    Err(_e) => continue,
                }
            }
        }
        Ok(())
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
    pub fn add_batch_nodes(mut self, mut addrs: Vec<SocketAddr>) -> KvsClientBuilder {
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
            ts: None,
            seq: self.seq,
            num_servers: servers.len(),
            servers,
            retries: self.retries,
            timeout: self.timeout,
            write_infos: Vec::new(),
        }
    }
}
