use crate::*;
use crate::{percolator::TimestampOracle, rpc::kvs_service::*};
use std::{net::SocketAddr, time::Duration};
use tonic::{Request, Response, Status};

/// Kvs Server
// #[derive(Clone)]
pub struct KvsBasicServer {
    store: MultiStore,
    addr: SocketAddr,
    ts_oracle: TimestampOracle,
}

impl KvsBasicServer {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: MultiStore, addr: SocketAddr, ts_oracle: TimestampOracle) -> Result<Self> {
        Ok(KvsBasicServer {
            store,
            addr,
            ts_oracle,
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
    fn lock_back_off_or_clean_up(&self, key: String, ts: u64) {
        if let Some((lock_key, lock_value)) = self.store.read_lock(key.clone(), None, Some(ts)) {
            let primary = lock_value.primary().to_owned();
            let primary_ts = lock_key.ts();
            if let Some((_pri_lock_key, pri_lock_value)) =
                self.store.read_lock(primary.clone(), None, Some(ts))
            {
                if pri_lock_value.elapsed() >= Duration::from_secs(3) {
                    self.store.erase_lock(primary, ts);
                    self.store.erase_lock(key, ts);
                }
            } else {
                if let Some((write_key, write_value)) =
                    self.store.read_write(primary, Some(primary_ts), None)
                {
                    if write_value.ts() == primary_ts {
                        let primary_commit_ts = write_key.ts();
                        self.store.write_write(
                            key.clone(),
                            primary_commit_ts,
                            primary_ts,
                            lock_value.op(),
                        );
                        self.store.erase_lock(key, primary_ts);
                    }
                } else {
                    self.store.erase_lock(key, ts);
                }
            }
        }
    }
}

#[tonic::async_trait]
impl KvRpc for KvsBasicServer {
    async fn get_timestamp(
        &self,
        request: Request<TsRequest>,
    ) -> std::result::Result<Response<TsReply>, Status> {
        let name = request.into_inner().name;
        let ts = self.ts_oracle.fetch_one().unwrap();
        let reply = TsReply { name, ts };
        Ok(tonic::Response::new(reply))
    }

    async fn txn_get(
        &self,
        req: Request<GetRequest>,
    ) -> std::result::Result<Response<GetReply>, Status> {
        let req = req.into_inner();
        loop {
            if self
                .store
                .read_lock(req.key.clone(), None, Some(req.ts))
                .is_some()
            {
                self.lock_back_off_or_clean_up(req.key.clone(), req.ts);
                continue;
            }
            let last_write = self.store.read_write(req.key.clone(), None, Some(req.ts));
            if last_write.is_none() || last_write.clone().unwrap().1.op() == WriteOp::Delete {
                return Err(KvRpcError::KeyNotFound)?;
            }
            let start_ts = last_write.unwrap().1.ts();
            let value = self
                .store
                .read_data(req.key.clone(), Some(start_ts), Some(start_ts))
                .unwrap()
                .1
                .value();
            let reply = GetReply {
                message: value,
                ts: req.ts,
                seq: req.seq,
            };
            return Ok(Response::new(reply));
        }
    }

    async fn txn_prewrite(
        &self,
        req: Request<PrewriteRequest>,
    ) -> std::result::Result<Response<PrewriteReply>, Status> {
        let req = req.into_inner();
        if self
            .store
            .read_write(req.key.clone(), Some(req.ts), None)
            .is_some()
        {
            return Err(KvRpcError::Abort(String::from("find write after ts")))?;
        }
        if self.store.read_lock(req.key.clone(), None, None).is_some() {
            return Err(KvRpcError::Abort(String::from("find another lock")))?;
        }
        self.store
            .write_data(req.key.clone(), req.ts, req.value.clone());
        self.store.write_lock(
            req.key.clone(),
            req.ts,
            req.primary.clone(),
            WriteOp::from_i32(req.op).unwrap(),
        );
        // for update primary ttl
        self.store.update_lock(req.primary.clone(), req.ts);
        let reply = PrewriteReply {
            ok: true,
            ts: req.ts,
            seq: req.seq,
        };
        Ok(Response::new(reply))
    }

    async fn txn_commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<Response<CommitReply>, Status> {
        let req = request.into_inner();
        if req.is_primary {
            if self
                .store
                .read_lock(req.primary, Some(req.start_ts), Some(req.start_ts))
                .is_none()
            {
                return Err(KvRpcError::Abort(String::from("primary lock missing")))?;
            }
        }
        self.store.write_write(
            req.key.clone(),
            req.commit_ts,
            req.start_ts,
            WriteOp::from_i32(req.op).unwrap(),
        );
        self.store.erase_lock(req.key, req.commit_ts);
        let reply = CommitReply {
            ok: true,
            ts: req.commit_ts,
            seq: req.seq,
        };
        Ok(Response::new(reply))
    }
}
