use crate::rpc::kvs_service::*;
use crate::*;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tonic::{Request, Response, Status};

/// Kvs Server
#[derive(Clone)]
pub struct KvsBasicServer {
    store: Arc<EngineKind>,
    addr: SocketAddr,
    ts_oracle: Arc<AtomicU64>,
}

impl KvsBasicServer {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: EngineKind, addr: SocketAddr) -> Result<Self> {
        Ok(KvsBasicServer {
            store: Arc::new(store),
            addr,
            ts_oracle: Arc::new(AtomicU64::new(1)),
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
    async fn get_timestamp(
        &self,
        request: Request<TsRequest>,
    ) -> std::result::Result<Response<TsReply>, Status> {
        let name = request.into_inner().name;
        let ts = self.ts_oracle.fetch_add(1, Ordering::SeqCst);
        let reply = TsReply { name, ts };
        Ok(tonic::Response::new(reply))
    }

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

    async fn prewrite(
        &self,
        _request: Request<PrewriteRequest>,
    ) -> std::result::Result<Response<PrewriteReply>, Status> {
        todo!()
    }

    async fn commit(
        &self,
        _request: Request<CommitRequest>,
    ) -> std::result::Result<Response<CommitReply>, Status> {
        todo!()
    }
}
