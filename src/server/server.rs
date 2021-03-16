use crate::rpc::kvs_service::*;
use crate::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Kvs Server
#[derive(Clone)]
pub struct KvsServer {
    store: Arc<EngineKind>,
}

impl KvsServer {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: EngineKind) -> Result<Self> {
        Ok(KvsServer {
            store: Arc::new(store),
        })
    }
}

#[tonic::async_trait]
impl KvRpc for KvsServer {
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
            .map_err(|_| Status::unknown("Failed"))
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
            .map_err(|e| Status::unknown(e.to_string()))
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
            .map_err(|e| Status::unknown(e.to_string()))
    }

    async fn init(
        &self,
        req: Request<SeqMessage>,
    ) -> std::result::Result<Response<SeqMessage>, Status> {
        let req = req.into_inner();
        Ok(Response::new(req))
    }
}
