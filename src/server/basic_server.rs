use crate::rpc::kvs_service::*;
use crate::*;
use std::{net::SocketAddr, sync::Arc};
use tonic::{Request, Response, Status};

/// Kvs Server
#[derive(Clone)]
pub struct KvsBasicServer {
    store: Arc<EngineKind>,
    addr: SocketAddr,
}

impl KvsBasicServer {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: EngineKind, addr: SocketAddr) -> Result<Self> {
        Ok(KvsBasicServer {
            store: Arc::new(store),
            addr,
        })
    }
    pub fn start(self) -> Result<()> {
        let threaded_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let handle = std::thread::spawn(move || {
            threaded_rt.block_on(async move {
                let addr = self.addr.clone();
                tonic::transport::Server::builder()
                    .add_service(KvRpcServer::new(self))
                    .serve(addr)
                    .await
                    .map_err(|e| KvError::StringError(e.to_string()))
            })
        });
        handle.join().unwrap()
    }
}

#[tonic::async_trait]
impl KvRpc for KvsBasicServer {
    async fn set(
        &self,
        req: Request<SetRequest>,
    ) -> std::result::Result<Response<SetReply>, Status> {
        let req = req.into_inner();
        self.store
            .set(req.key.clone(), req.value.clone())
            .map(|_| SetReply {
                message: "OK".to_string(),
                name: req.name,
                seq: req.seq,
            })
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }
    async fn get(
        &self,
        req: Request<GetRequest>,
    ) -> std::result::Result<Response<GetReply>, Status> {
        let req = req.into_inner();
        self.store
            .get(req.key.clone())
            .map(|value| GetReply {
                message: value.unwrap_or("Key not found".to_string()),
                name: req.name,
                seq: req.seq,
            })
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }
    async fn remove(
        &self,
        req: Request<RemoveRequest>,
    ) -> std::result::Result<Response<RemoveReply>, Status> {
        let req = req.into_inner();
        self.store
            .remove(req.key.clone())
            .map(|_| RemoveReply {
                message: "OK".to_string(),
                name: req.name,
                seq: req.seq,
            })
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }

    async fn init(
        &self,
        req: Request<SeqMessage>,
    ) -> std::result::Result<Response<SeqMessage>, Status> {
        let req = req.into_inner();
        Ok(Response::new(req))
    }
}
