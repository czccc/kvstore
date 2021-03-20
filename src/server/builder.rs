use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use tokio::sync::mpsc::unbounded_channel;
use tonic::transport::Channel;

use super::*;
use crate::preclude::*;

use super::{KvsServer, ServerKind};

struct ServerNodeInfo {
    id: usize,
    addr: SocketAddr,
    path: PathBuf,
}
/// KvsServer Builder that can set:
///   - store engine, option: ["kvs", "sled"]
///   - server kind, option: ["basic", "raft"]
///   - root path, which can simplify configuration
///   - server info: which included SocketAddr and running path
pub struct KvsServerBuilder {
    info: Vec<ServerNodeInfo>,
    store_kind: String,
    server_kind: String,
    root_path: PathBuf,
}

impl Default for KvsServerBuilder {
    fn default() -> Self {
        Self {
            info: Vec::new(),
            store_kind: String::from("kvs"),
            server_kind: String::from("basic"),
            root_path: std::env::current_dir().unwrap(),
        }
    }
}

impl KvsServerBuilder {
    /// set the store engine
    pub fn set_engine(mut self, engine: String) -> Self {
        self.store_kind = engine;
        self
    }
    /// set the server kind
    pub fn set_server(mut self, server: String) -> Self {
        self.server_kind = server;
        self
    }
    /// set the root path
    pub fn set_root_path(mut self, path: PathBuf) -> Self {
        self.root_path = path;
        self
    }
    /// add one node and its addr and path
    pub fn add_node(mut self, addr: SocketAddr, path: PathBuf) -> Self {
        let node = ServerNodeInfo {
            id: self.info.len(),
            addr,
            path,
        };
        self.info.push(node);
        self
    }
    /// add a batch od nodes of given vector of addr, node path will be in root path
    pub fn add_batch_nodes(mut self, addrs: Vec<SocketAddr>) -> Self {
        addrs
            .into_iter()
            .map(|addr| {
                let node = ServerNodeInfo {
                    id: self.info.len(),
                    addr,
                    path: self.root_path.join(format!("server-{}", self.info.len())),
                };
                self.info.push(node);
            })
            .for_each(|_| ());
        self
    }

    /// consume this builder, create a new KvsServer
    pub fn build(self) -> KvsServer {
        match self.server_kind.as_ref() {
            "basic" => self.build_basic_server(),
            "raft" => self.build_raft_server(),
            _unknown => unreachable!(),
        }
    }

    fn build_raft_server(self) -> KvsServer {
        assert!(self.info.len() > 1 && self.info.len() % 2 != 0);
        let peers: Vec<RaftRpcClient<Channel>> = self
            .info
            .iter()
            .map(|node| format!("http://{}", node.addr))
            .map(|node| Channel::from_shared(node).unwrap().connect_lazy().unwrap())
            .map(|res| RaftRpcClient::new(res))
            .collect();
        let ts_oracle = TimestampOracle::open(self.root_path.clone()).unwrap();
        let nodes: Vec<(RaftNode, KvRaftNode, SocketAddr)> = self
            .info
            .iter()
            .map(|info| {
                let per = Arc::new(FilePersister::with_path(info.path.clone()));
                let (tx, rx) = unbounded_channel();
                let raft = RaftNode::new(peers.clone(), info.id, per.clone(), tx);
                let store = MultiStore::new(info.path.clone(), self.store_kind.clone());
                let kv_raft = KvRaftNode::new(
                    raft.clone(),
                    store,
                    info.id,
                    per,
                    Some(1024),
                    rx,
                    ts_oracle.clone(),
                );
                (raft, kv_raft, info.addr)
            })
            .collect();
        let server = KvRaftServer::new(nodes);
        KvsServer::new(ServerKind::Raft(server))
    }
    fn build_basic_server(self) -> KvsServer {
        assert!(self.info.len() == 1);
        let info = self.info.first().unwrap();
        let store = MultiStore::new(info.path.clone(), self.store_kind.clone());
        let ts_oracle = TimestampOracle::open(info.path.clone()).unwrap();
        let server = KvsBasicServer::new(store, info.addr, ts_oracle).unwrap();
        KvsServer::new(ServerKind::Basic(server))
    }
}
