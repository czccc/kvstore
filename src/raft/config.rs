use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use crate::preclude::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tonic::transport::Channel;

use super::raft::ApplyMsg;

struct RaftNodeInfo {
    id: usize,
    addr: SocketAddr,
    path: PathBuf,
}
pub struct RaftConfig {
    info: Vec<RaftNodeInfo>,
    store_kind: String,
}

impl RaftConfig {
    pub fn new() -> Self {
        Self {
            info: Vec::new(),
            store_kind: String::from("kvs"),
        }
    }
    pub fn set_engine(&mut self, engine: String) {
        self.store_kind = engine;
    }
    pub fn add_raft_node(&mut self, addr: SocketAddr, path: Option<PathBuf>) {
        let node = RaftNodeInfo {
            id: self.info.len(),
            addr,
            path: path.unwrap_or_default(),
        };
        self.info.push(node);
    }
    fn build_persisters(&mut self) -> Vec<Arc<FilePersister>> {
        self.info
            .iter()
            .map(|node| FilePersister::with_path(node.path.clone()))
            .map(|per| Arc::new(per))
            .collect()
    }
    fn build_lazy_channels(&mut self) -> Vec<RaftRpcClient<Channel>> {
        self.info
            .iter()
            .map(|node| format!("http://{}", node.addr))
            .map(|node| Channel::from_shared(node).unwrap().connect_lazy().unwrap())
            .map(|res| RaftRpcClient::new(res))
            .collect()
    }
    fn build_raft_node(
        &mut self,
        persisters: Vec<Arc<FilePersister>>,
    ) -> Vec<(RaftNode, UnboundedReceiver<ApplyMsg>)> {
        let peers = self.build_lazy_channels();
        self.info
            .iter()
            .zip(persisters.iter())
            .map(|(node, per)| {
                let (tx, rx) = unbounded_channel();
                let node = RaftNode::new(peers.clone(), node.id, per.clone(), tx);
                (node, rx)
            })
            .collect()
    }
    pub fn build_kv_raft_clients(&mut self) -> Vec<KvRpcClient<Channel>> {
        self.info
            .iter()
            .map(|node| format!("http://{}", node.addr))
            .map(|node| Channel::from_shared(node).unwrap().connect_lazy().unwrap())
            .map(|res| KvRpcClient::new(res))
            .collect()
    }
    pub fn build_kv_raft_servers(&mut self) -> Vec<(RaftNode, KvRaftNode, SocketAddr)> {
        let persisters = self.build_persisters();
        let raft_nodes = self.build_raft_node(persisters.clone());
        self.info
            .iter()
            .zip(persisters.into_iter())
            .zip(raft_nodes.into_iter())
            .map(|((info, per), (raft, rx))| {
                let store = match self.store_kind.as_ref() {
                    "kvs" => EngineKind::kvs(KvStore::open(info.path.to_owned()).unwrap()),
                    "sled" => EngineKind::sled(KvSled::open(info.path.to_owned()).unwrap()),
                    _unknown => unreachable!(),
                };
                let kv_raft = KvRaftNode::new(raft.clone(), store, info.id, per, Some(1024), rx);
                (raft, kv_raft, info.addr)
            })
            .collect()
    }
}
