use std::{
    collections::HashMap,
    sync::Arc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    task::Poll,
    thread,
    time::Duration,
};

use crate::{percolator::TimestampOracle, rpc::kvs_service::*, KvRpcError, MultiStore};
use prost::Message;
use tonic::{Request, Response, Status};

use super::{persister, raft::ApplyMsg};
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot::{channel, Sender},
    },
    time::timeout,
};
use tokio_stream::{Stream, StreamExt};

use super::raft;

type RpcResult<T> = std::result::Result<T, KvRpcError>;

pub struct KvRaftInner {
    pub rf: raft::RaftNode,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    apply_ch: UnboundedReceiver<ApplyMsg>,

    // DB
    store: MultiStore,
    pending: HashMap<(u64, u64), KvEvent>,
    last_index: HashMap<u64, Arc<AtomicU64>>,
    // Stream
    receiver: UnboundedReceiver<KvEvent>,
}

impl std::fmt::Display for KvRaftInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let desp = if self.rf.is_leader() {
            String::from("Leader")
        } else {
            String::from("Follow")
        };
        write!(f, "[{} {}]", desp, self.me)
    }
}

impl KvRaftInner {
    pub fn new(
        store: MultiStore,
        rf: raft::RaftNode,
        me: usize,
        persister: Arc<dyn persister::Persister>,
        maxraftstate: Option<usize>,
        receiver: UnboundedReceiver<KvEvent>,
        apply_ch: UnboundedReceiver<ApplyMsg>,
    ) -> KvRaftInner {
        let snapshot = persister.snapshot();

        let mut server = KvRaftInner {
            me,
            rf,
            maxraftstate,
            apply_ch,
            store,
            pending: HashMap::new(),
            last_index: HashMap::new(),
            receiver,
        };
        server.restore_from_snapshot(snapshot);
        server
    }

    fn create_snapshot(&self) -> Vec<u8> {
        let data = self.store.export().unwrap();
        let snapshot = Snapshot {
            d_keys: data.get(0).unwrap().clone(),
            d_values: data.get(1).unwrap().clone(),
            l_keys: data.get(2).unwrap().clone(),
            l_values: data.get(3).unwrap().clone(),
            w_keys: data.get(4).unwrap().clone(),
            w_values: data.get(5).unwrap().clone(),
            timestamps: self.last_index.keys().cloned().collect(),
            seqs: self
                .last_index
                .values()
                .cloned()
                .map(|v| v.load(Ordering::SeqCst))
                .collect(),
        };
        let mut buf = Vec::new();
        snapshot.encode(&mut buf).unwrap();
        buf
    }

    fn restore_from_snapshot(&mut self, snapshot: Vec<u8>) {
        if let Ok(Snapshot {
            d_keys,
            d_values,
            l_keys,
            l_values,
            w_keys,
            w_values,
            timestamps,
            seqs,
        }) = Snapshot::decode(&*snapshot)
        {
            let data = vec![d_keys, d_values, l_keys, l_values, w_keys, w_values];
            self.store.import(data).unwrap();
            let last_index: HashMap<u64, Arc<AtomicU64>> = timestamps
                .into_iter()
                .zip(seqs.into_iter().map(|v| Arc::new(AtomicU64::new(v))))
                .collect();
            self.last_index = last_index;
        }
    }
    fn handle_apply_msg(&mut self, msg: ApplyMsg) {
        if msg.command.is_empty() {
            return;
        }
        if !msg.command_valid {
            debug!("{} recv [Snapshot]", self);
            self.restore_from_snapshot(msg.command);
            return;
        }
        info!(
            "{} recv [ApplyMsg {} {}]",
            self, msg.command_valid, msg.command_index
        );
        let _index = msg.command_index;
        if let Ok(req) = GetRequest::decode(&*msg.command) {
            self.handle_txn_get(req);
        }
        if let Ok(req) = PrewriteRequest::decode(&*msg.command) {
            self.handle_txn_prewrite(req);
        }
        if let Ok(req) = CommitRequest::decode(&*msg.command) {
            self.handle_txn_commit(req);
        }
    }
}

impl KvRaftInner {
    fn handle_txn_get(&mut self, req: GetRequest) {
        let tx = {
            if let Some(KvEvent::TxnGet(_args, tx)) = self.pending.remove(&(req.ts, req.seq)) {
                Some(tx)
            } else {
                None
            }
        };
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
                tx.map(|tx| tx.send(Err(KvRpcError::KeyNotFound)).unwrap());
                return;
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
            tx.map(|tx| tx.send(Ok(reply)).unwrap());

            return;
        }
    }
    fn handle_txn_prewrite(&mut self, req: PrewriteRequest) {
        let tx = {
            if let Some(KvEvent::TxnPrewrite(_args, tx)) = self.pending.remove(&(req.ts, req.seq)) {
                Some(tx)
            } else {
                None
            }
        };
        if self.check_duplicate(req.ts, req.seq).is_err() {
            return;
        }
        if req.seq > self.last_index[&req.ts].load(Ordering::SeqCst) {
            self.last_index[&req.ts].store(req.seq, Ordering::SeqCst);
            if self
                .store
                .read_write(req.key.clone(), Some(req.ts), None)
                .is_some()
            {
                tx.map(|tx| {
                    tx.send(Err(KvRpcError::Abort(String::from("find write after ts"))))
                        .unwrap()
                });
                return;
            }
            if self.store.read_lock(req.key.clone(), None, None).is_some() {
                tx.map(|tx| {
                    tx.send(Err(KvRpcError::Abort(String::from("find another lock"))))
                        .unwrap()
                });
                return;
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
            tx.map(|tx| tx.send(Ok(reply)).unwrap());
        }
    }
    fn handle_txn_commit(&mut self, req: CommitRequest) {
        let tx = {
            if let Some(KvEvent::TxnCommit(_args, tx)) =
                self.pending.remove(&(req.commit_ts, req.seq))
            {
                Some(tx)
            } else {
                None
            }
        };
        if self.check_duplicate(req.commit_ts, req.seq).is_err() {
            return;
        }
        if req.seq > self.last_index[&req.commit_ts].load(Ordering::SeqCst) {
            self.last_index[&req.commit_ts].store(req.seq, Ordering::SeqCst);
            if req.is_primary {
                if self
                    .store
                    .read_lock(req.primary, Some(req.start_ts), Some(req.start_ts))
                    .is_none()
                {
                    tx.map(|tx| {
                        tx.send(Err(KvRpcError::Abort(String::from("primary lock missing"))))
                            .unwrap()
                    });
                    return;
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
            tx.map(|tx| tx.send(Ok(reply)).unwrap());
        }
    }
}

impl KvRaftInner {
    fn check_duplicate(&mut self, ts: u64, seq: u64) -> RpcResult<()> {
        if self.last_index.get(&ts).is_none() {
            self.last_index.insert(ts, Arc::new(AtomicU64::new(0)));
        }
        if self.last_index.get(&ts).unwrap().load(Ordering::SeqCst) >= seq {
            Err(KvRpcError::DuplicatedRequest)
        } else {
            Ok(())
        }
    }
    fn lock_back_off_or_clean_up(&mut self, key: String, ts: u64) {
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

#[derive(Debug)]
pub enum KvEvent {
    TxnGet(GetRequest, Sender<RpcResult<GetReply>>),
    TxnPrewrite(PrewriteRequest, Sender<RpcResult<PrewriteReply>>),
    TxnCommit(CommitRequest, Sender<RpcResult<CommitReply>>),
}

impl Stream for KvRaftInner {
    type Item = ();

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                debug!("{} Executor recv [Event]", self);
                return match event {
                    KvEvent::TxnGet(args, sender) => {
                        if let Err(e) = self.check_duplicate(args.ts, args.seq) {
                            sender.send(Err(e)).unwrap();
                        } else if let Ok((_index, _term)) = self.rf.start_read_only(&args) {
                            let (tx, rx) = channel();
                            self.pending
                                .insert((args.ts, args.seq), KvEvent::TxnGet(args.clone(), tx));
                            let last_index = self.last_index.get(&args.ts).unwrap().clone();
                            tokio::spawn(async move {
                                let reply = match timeout(Duration::from_millis(3000), rx).await {
                                    Ok(Ok(Ok(reply))) => {
                                        last_index.store(reply.seq, Ordering::SeqCst);
                                        Ok(reply)
                                    }
                                    Ok(Ok(Err(status))) => Err(status),
                                    Ok(Err(_e)) => Err(KvRpcError::Recv),
                                    Err(_e) => Err(KvRpcError::Timeout),
                                };
                                sender.send(reply).unwrap_or(());
                            });
                        } else {
                            sender.send(Err(KvRpcError::NotLeader)).unwrap_or(());
                        }
                        Poll::Ready(Some(()))
                    }
                    KvEvent::TxnPrewrite(args, sender) => {
                        if let Err(e) = self.check_duplicate(args.ts, args.seq) {
                            sender.send(Err(e)).unwrap();
                        } else if let Ok((_index, _term)) = self.rf.start(&args) {
                            info!("prewrite 1");
                            let (tx, rx) = channel();
                            self.pending.insert(
                                (args.ts, args.seq),
                                KvEvent::TxnPrewrite(args.clone(), tx),
                            );
                            let last_index = self.last_index.get(&args.ts).unwrap().clone();
                            tokio::spawn(async move {
                                let reply = match timeout(Duration::from_millis(3000), rx).await {
                                    Ok(Ok(Ok(reply))) => {
                                        last_index.store(reply.seq, Ordering::SeqCst);
                                        Ok(reply)
                                    }
                                    Ok(Ok(Err(status))) => Err(status),
                                    Ok(Err(_e)) => Err(KvRpcError::Recv),
                                    Err(_e) => Err(KvRpcError::Timeout),
                                };
                                sender.send(reply).unwrap_or(());
                            });
                        } else {
                            sender.send(Err(KvRpcError::NotLeader)).unwrap_or(());
                        }
                        Poll::Ready(Some(()))
                    }
                    KvEvent::TxnCommit(args, sender) => {
                        if let Err(e) = self.check_duplicate(args.commit_ts, args.seq) {
                            sender.send(Err(e)).unwrap();
                        } else if let Ok((_index, _term)) = self.rf.start(&args) {
                            let (tx, rx) = channel();
                            self.pending.insert(
                                (args.commit_ts, args.seq),
                                KvEvent::TxnCommit(args.clone(), tx),
                            );
                            let last_index = self.last_index.get(&args.commit_ts).unwrap().clone();
                            tokio::spawn(async move {
                                let reply = match timeout(Duration::from_millis(3000), rx).await {
                                    Ok(Ok(Ok(reply))) => {
                                        last_index.store(reply.seq, Ordering::SeqCst);
                                        Ok(reply)
                                    }
                                    Ok(Ok(Err(status))) => Err(status),
                                    Ok(Err(_e)) => Err(KvRpcError::Recv),
                                    Err(_e) => Err(KvRpcError::Timeout),
                                };
                                sender.send(reply).unwrap_or(());
                            });
                        } else {
                            sender.send(Err(KvRpcError::NotLeader)).unwrap_or(());
                        }
                        Poll::Ready(Some(()))
                    }
                };
            }
            Poll::Ready(None) => {}
            Poll::Pending => {}
        }
        match self.apply_ch.poll_recv(cx) {
            Poll::Ready(Some(msg)) => {
                self.handle_apply_msg(msg);
                Poll::Ready(Some(()))
            }
            Poll::Ready(None) => Poll::Ready(Some(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// A KvRaftNode that owned a store engine and can handle RPC request from client
#[derive(Clone)]
pub struct KvRaftNode {
    handle: Arc<Mutex<thread::JoinHandle<()>>>,
    sender: UnboundedSender<KvEvent>,
    ts_oracle: TimestampOracle,
}

impl KvRaftNode {
    /// Create a new KvRaftNode
    pub fn new(
        rf: raft::RaftNode,
        store: MultiStore,
        me: usize,
        persister: Arc<dyn persister::Persister>,
        maxraftstate: Option<usize>,
        apply_ch: UnboundedReceiver<ApplyMsg>,
        ts_oracle: TimestampOracle,
    ) -> KvRaftNode {
        let (sender, receiver) = unbounded_channel();
        let mut kv_raft =
            KvRaftInner::new(store, rf, me, persister, maxraftstate, receiver, apply_ch);

        let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = thread::Builder::new()
            .name(format!("KvServerNode-{}", me))
            .spawn(move || {
                threaded_rt.block_on(async move {
                    debug!("Enter KvRaftInner main executor!");
                    while kv_raft.next().await.is_some() {
                        trace!("KvRaftInner: Get event");
                    }
                    debug!("Leave KvRaftInner main executor!");
                })
            })
            .unwrap();
        KvRaftNode {
            handle: Arc::new(Mutex::new(handle)),
            sender,
            ts_oracle,
        }
    }
}

#[tonic::async_trait]
impl KvRpc for KvRaftNode {
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
        let (tx, rx) = channel();
        self.sender.send(KvEvent::TxnGet(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|_| Err(KvRpcError::Recv))
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }

    async fn txn_prewrite(
        &self,
        request: Request<PrewriteRequest>,
    ) -> std::result::Result<Response<PrewriteReply>, Status> {
        let req = request.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::TxnPrewrite(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|_| Err(KvRpcError::Recv))
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }

    async fn txn_commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<Response<CommitReply>, Status> {
        let req = request.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::TxnCommit(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|_| Err(KvRpcError::Recv))
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }
}
