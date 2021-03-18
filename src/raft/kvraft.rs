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

use crate::{rpc::kvs_service::*, KvRpcError, MultiStore};
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
        // let desp = if self.rf.is_leader() {
        //     String::from("Leader")
        // } else {
        //     String::from("Follow")
        // };
        write!(f, "[{} {}]", "Server", self.me)
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
        // let (tx, apply_ch) = unbounded_channel();
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
        todo!()
        // let (keys, values) = self.store.export().unwrap();
        // let snapshot = Snapshot {
        //     keys,
        //     values,
        //     clients: self.last_index.keys().cloned().collect(),
        //     seqs: self
        //         .last_index
        //         .values()
        //         .cloned()
        //         .map(|v| v.load(Ordering::SeqCst))
        //         .collect(),
        // };
        // let mut buf = Vec::new();
        // snapshot.encode(&mut buf).unwrap();
        // buf
    }

    fn restore_from_snapshot(&mut self, _snapshot: Vec<u8>) {
        todo!()
        // if let Ok(Snapshot {
        //     keys,
        //     values,
        //     clients,
        //     seqs,
        // }) = Snapshot::decode(&*snapshot)
        // {
        //     self.store.import((keys, values)).unwrap();
        //     let last_index: HashMap<String, Arc<AtomicU64>> = clients
        //         .into_iter()
        //         .zip(seqs.into_iter().map(|v| Arc::new(AtomicU64::new(v))))
        //         .collect();
        //     self.last_index = last_index;
        // }
    }
    fn handle_set(&mut self, args: SetRequest, sender: Sender<RpcResult<SetReply>>) {}
    fn handle_get(&mut self, args: GetRequest, sender: Sender<RpcResult<GetReply>>) {}
    fn handle_remove(&mut self, args: RemoveRequest, sender: Sender<RpcResult<RemoveReply>>) {}

    fn handle_apply_msg(&mut self, msg: ApplyMsg) {
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
        if let Ok(_req) = SetRequest::decode(&*msg.command) {
            // if !self.last_index.contains_key(&req.name) {
            //     self.last_index
            //         .insert(req.name.clone(), Arc::new(AtomicU64::new(0)));
            // }
            // if let Some(KvEvent::Set(args, tx)) = self.pending.remove(&index) {
            //     if req.seq == args.seq
            //         && req.seq > self.last_index[&req.name].load(Ordering::SeqCst)
            //     {
            //         self.last_index[&req.name].store(req.seq, Ordering::SeqCst);
            //         let reply = self
            //             .store
            //             .set(req.key, req.value)
            //             .map(|_| SetReply {
            //                 message: "OK".to_owned(),
            //                 name: args.name,
            //                 seq: args.seq,
            //             })
            //             .map_err(|e| Status::internal(e.to_string()));
            //         return tx.send(reply).unwrap_or(());
            //     } else {
            //         return tx.send(Err(KvRpcError::DuplicatedRequest)).unwrap_or(());
            //     }
            // }
        }
        if let Ok(_req) = GetRequest::decode(&*msg.command) {
            // if !self.last_index.contains_key(&req.name) {
            //     self.last_index
            //         .insert(req.name.clone(), Arc::new(AtomicU64::new(0)));
            // }
            // if let Some(KvEvent::Get(args, tx)) = self.pending.remove(&index) {
            //     if req.seq == args.seq
            //         && req.seq > self.last_index[&req.name].load(Ordering::SeqCst)
            //     {
            //         self.last_index[&req.name].store(req.seq, Ordering::SeqCst);
            //         let reply = self
            //             .store
            //             .get(req.key)
            //             .map(|value| GetReply {
            //                 message: value.unwrap_or(String::from("Key not found")),
            //                 name: args.name,
            //                 seq: args.seq,
            //             })
            //             .map_err(|e| Status::internal(e.to_string()));
            //         tx.send(reply).unwrap_or(());
            //     } else {
            //         return tx
            //             .send(Err(Status::already_exists("Duplicated Request")))
            //             .unwrap_or(());
            //     }
            // }
        }
        if let Ok(_req) = RemoveRequest::decode(&*msg.command) {
            // if !self.last_index.contains_key(&req.name) {
            //     self.last_index
            //         .insert(req.name.clone(), Arc::new(AtomicU64::new(0)));
            // }
            // if let Some(KvEvent::Remove(args, tx)) = self.pending.remove(&index) {
            //     if req.seq == args.seq
            //         && req.seq > self.last_index[&req.name].load(Ordering::SeqCst)
            //     {
            //         self.last_index[&req.name].store(req.seq, Ordering::SeqCst);
            //         let reply = self
            //             .store
            //             .remove(req.key)
            //             .map(|_| RemoveReply {
            //                 message: String::from("OK"),
            //                 name: args.name,
            //                 seq: args.seq,
            //             })
            //             .map_err(|e| Status::internal(e.to_string()));
            //         tx.send(reply).unwrap_or(());
            //     } else {
            //         return tx
            //             .send(Err(Status::already_exists("Duplicated Request")))
            //             .unwrap_or(());
            //     }
            // }
        }
        if let Ok(req) = PrewriteRequest::decode(&*msg.command) {
            self.handle_prewrite(req);
        }
        if let Ok(req) = CommitRequest::decode(&*msg.command) {
            self.handle_commit(req);
        }
    }
}

impl KvRaftInner {
    fn handle_prewrite(&mut self, req: PrewriteRequest) {
        if let Some(KvEvent::Prewrite(_args, tx)) = self.pending.remove(&(req.ts, req.seq)) {
            if req.seq > self.last_index[&req.ts].load(Ordering::SeqCst) {
                self.last_index[&req.ts].store(req.seq, Ordering::SeqCst);
                if self
                    .store
                    .read_write(req.key.clone(), Some(req.ts), None)
                    .is_some()
                {
                    tx.send(Err(KvRpcError::Abort(String::from("find write after ts"))))
                        .unwrap();
                    return;
                }
                if self.store.read_lock(req.key.clone(), None, None).is_some() {
                    tx.send(Err(KvRpcError::Abort(String::from("find another lock"))))
                        .unwrap();
                    return;
                }
                self.store
                    .write_data(req.key.clone(), req.ts, req.value.clone());
                self.store
                    .write_lock(req.key.clone(), req.ts, req.primary.clone());
                let reply = PrewriteReply {
                    ok: true,
                    seq: req.seq,
                };
                tx.send(Ok(reply)).unwrap();
            }
        }
    }
    fn handle_commit(&mut self, req: CommitRequest) {
        if let Some(KvEvent::Commit(_args, tx)) = self.pending.remove(&(req.commit_ts, req.seq)) {
            if req.seq > self.last_index[&req.commit_ts].load(Ordering::SeqCst) {
                self.last_index[&req.commit_ts].store(req.seq, Ordering::SeqCst);
                if req.is_primary {
                    if self
                        .store
                        .read_lock(req.primary, Some(req.start_ts), Some(req.start_ts))
                        .is_none()
                    {
                        tx.send(Err(KvRpcError::Abort(String::from("primary lock missing"))))
                            .unwrap();
                        return;
                    }
                }
                self.store
                    .write_write(req.key.clone(), req.commit_ts, req.start_ts);
                self.store.erase_lock(req.key, req.commit_ts);
                let reply = CommitReply {
                    ok: true,
                    seq: req.seq,
                };
                tx.send(Ok(reply)).unwrap();
            }
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
}

#[derive(Debug)]
pub enum KvEvent {
    Set(SetRequest, Sender<RpcResult<SetReply>>),
    Get(GetRequest, Sender<RpcResult<GetReply>>),
    Remove(RemoveRequest, Sender<RpcResult<RemoveReply>>),
    Prewrite(PrewriteRequest, Sender<RpcResult<PrewriteReply>>),
    Commit(CommitRequest, Sender<RpcResult<CommitReply>>),
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
                    KvEvent::Set(args, tx) => {
                        self.handle_set(args, tx);
                        Poll::Ready(Some(()))
                    }
                    KvEvent::Get(args, tx) => {
                        self.handle_get(args, tx);
                        Poll::Ready(Some(()))
                    }
                    KvEvent::Remove(args, tx) => {
                        self.handle_remove(args, tx);
                        Poll::Ready(Some(()))
                    }
                    KvEvent::Prewrite(args, sender) => {
                        if let Err(e) = self.check_duplicate(args.ts, args.seq) {
                            sender.send(Err(e)).unwrap();
                        } else if let Ok((_index, _term)) = self.rf.start(&args) {
                            let (tx, rx) = channel();
                            self.pending
                                .insert((args.ts, args.seq), KvEvent::Prewrite(args.clone(), tx));
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
                    KvEvent::Commit(args, sender) => {
                        if let Err(e) = self.check_duplicate(args.commit_ts, args.seq) {
                            sender.send(Err(e)).unwrap();
                        } else if let Ok((_index, _term)) = self.rf.start(&args) {
                            let (tx, rx) = channel();
                            self.pending.insert(
                                (args.commit_ts, args.seq),
                                KvEvent::Commit(args.clone(), tx),
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
    ts_oracle: Arc<AtomicU64>,
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
        ts_oracle: Arc<AtomicU64>,
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
        let ts = self.ts_oracle.fetch_add(1, Ordering::SeqCst);
        let reply = TsReply { name, ts };
        Ok(tonic::Response::new(reply))
    }

    async fn set(
        &self,
        req: Request<SetRequest>,
    ) -> std::result::Result<Response<SetReply>, Status> {
        let req = req.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::Set(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|_| Err(KvRpcError::Recv))
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }
    async fn get(
        &self,
        req: Request<GetRequest>,
    ) -> std::result::Result<Response<GetReply>, Status> {
        let req = req.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::Get(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|_| Err(KvRpcError::Recv))
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }
    async fn remove(
        &self,
        req: Request<RemoveRequest>,
    ) -> std::result::Result<Response<RemoveReply>, Status> {
        let req = req.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::Remove(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|_| Err(KvRpcError::Recv))
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }

    async fn prewrite(
        &self,
        request: Request<PrewriteRequest>,
    ) -> std::result::Result<Response<PrewriteReply>, Status> {
        let req = request.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::Prewrite(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|_| Err(KvRpcError::Recv))
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<Response<CommitReply>, Status> {
        let req = request.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::Commit(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|_| Err(KvRpcError::Recv))
            .map(|reply| Response::new(reply))
            .map_err(|e| e.into())
    }
}
