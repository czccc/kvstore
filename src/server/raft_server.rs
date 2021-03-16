use crate::preclude::*;
use std::net::SocketAddr;

pub struct KvRaftServer {
    num_peers: usize,
    nodes: Vec<(RaftNode, KvRaftNode, SocketAddr)>,
}

impl KvRaftServer {
    pub fn new(nodes: Vec<(RaftNode, KvRaftNode, SocketAddr)>) -> KvRaftServer {
        KvRaftServer {
            num_peers: nodes.len(),
            nodes,
        }
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
