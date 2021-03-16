use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use tokio::sync::mpsc::unbounded_channel;
use tonic::transport::Channel;

use crate::preclude::*;

pub struct KvRaftServer {
    num_peers: usize,
    nodes: Vec<(RaftNode, KvRaftNode, SocketAddr)>,
}

impl KvRaftServer {
    pub fn builder() -> KvRaftServerBuilder {
        KvRaftServerBuilder::new()
    }
    pub fn start(self) -> Result<()> {
        let handles = self
            .nodes
            .into_iter()
            .map(|(rf, kvrf, addr)| {
                let threaded_rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                std::thread::spawn(move || {
                    threaded_rt.block_on(async move {
                        tonic::transport::Server::builder()
                            .add_service(RaftRpcServer::new(rf))
                            .add_service(KvRpcServer::new(kvrf))
                            .serve(addr)
                            .await
                            .map_err(|e| KvError::StringError(e.to_string()))
                    })
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap()?;
        }
        Ok(())
    }
}

struct RaftNodeInfo {
    id: usize,
    addr: SocketAddr,
    path: PathBuf,
}

pub struct KvRaftServerBuilder {
    info: Vec<RaftNodeInfo>,
    store_kind: String,
}

impl KvRaftServerBuilder {
    pub fn new() -> Self {
        Self {
            info: Vec::new(),
            store_kind: String::from("kvs"),
        }
    }
    pub fn set_engine(mut self, engine: String) -> KvRaftServerBuilder {
        self.store_kind = engine;
        self
    }
    pub fn add_node(mut self, addr: SocketAddr, path: PathBuf) -> KvRaftServerBuilder {
        let node = RaftNodeInfo {
            id: self.info.len(),
            addr,
            path,
        };
        self.info.push(node);
        self
    }
    pub fn build(self) -> KvRaftServer {
        let peers: Vec<RaftRpcClient<Channel>> = self
            .info
            .iter()
            .map(|node| format!("http://{}", node.addr))
            .map(|node| Channel::from_shared(node).unwrap().connect_lazy().unwrap())
            .map(|res| RaftRpcClient::new(res))
            .collect();
        let nodes: Vec<(RaftNode, KvRaftNode, SocketAddr)> = self
            .info
            .iter()
            .map(|info| {
                let per = Arc::new(FilePersister::with_path(info.path.clone()));
                let (tx, rx) = unbounded_channel();
                let raft = RaftNode::new(peers.clone(), info.id, per.clone(), tx);
                let store = match self.store_kind.as_ref() {
                    "kvs" => EngineKind::kvs(KvStore::open(info.path.to_owned()).unwrap()),
                    "sled" => EngineKind::sled(KvSled::open(info.path.to_owned()).unwrap()),
                    _unknown => unreachable!(),
                };
                let kv_raft = KvRaftNode::new(raft.clone(), store, info.id, per, Some(1024), rx);
                (raft, kv_raft, info.addr)
            })
            .collect();
        KvRaftServer {
            num_peers: nodes.len(),
            nodes,
        }
    }
}
