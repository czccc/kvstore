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

use crate::{rpc::kvs_service::*, EngineKind};
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

pub struct KvRaftInner {
    pub rf: raft::RaftNode,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    apply_ch: UnboundedReceiver<ApplyMsg>,

    // DB
    store: EngineKind,
    pending: HashMap<u64, KvEvent>,
    last_index: HashMap<String, Arc<AtomicU64>>,
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
        store: EngineKind,
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
        let (keys, values) = self.store.export().unwrap();
        let snapshot = Snapshot {
            keys,
            values,
            clients: self.last_index.keys().cloned().collect(),
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
            keys,
            values,
            clients,
            seqs,
        }) = Snapshot::decode(&*snapshot)
        {
            self.store.import((keys, values)).unwrap();
            let last_index: HashMap<String, Arc<AtomicU64>> = clients
                .into_iter()
                .zip(seqs.into_iter().map(|v| Arc::new(AtomicU64::new(v))))
                .collect();
            self.last_index = last_index;
        }
    }
    fn handle_set(&mut self, args: SetRequest, sender: Sender<Result<SetReply, Status>>) {
        if !self.last_index.contains_key(&args.name) {
            self.last_index
                .insert(args.name.clone(), Arc::new(AtomicU64::new(0)));
        }
        let last_index = self.last_index[&args.name].clone();
        if args.seq < last_index.load(Ordering::SeqCst) {
            return sender
                .send(Err(Status::already_exists("Duplicated Request")))
                .unwrap_or(());
        }
        let res = self.rf.start(&args);
        if res.is_err() {
            return sender
                .send(Err(Status::permission_denied("Not Leader")))
                .unwrap_or(());
        }
        let (index, _term) = res.unwrap();
        let (tx, rx) = channel();
        self.pending.insert(index, KvEvent::Set(args, tx));

        tokio::spawn(async move {
            let reply = match timeout(Duration::from_millis(3000), rx).await {
                Ok(Ok(Ok(reply))) => {
                    last_index.store(reply.seq, Ordering::SeqCst);
                    Ok(reply)
                }
                Ok(Ok(Err(status))) => Err(status),
                Ok(Err(_e)) => Err(Status::cancelled("Recv Error")),
                Err(_e) => Err(Status::deadline_exceeded("Timeout")),
            };
            sender.send(reply).unwrap_or(());
        });
    }
    fn handle_get(&mut self, args: GetRequest, sender: Sender<Result<GetReply, Status>>) {
        if !self.last_index.contains_key(&args.name) {
            self.last_index
                .insert(args.name.clone(), Arc::new(AtomicU64::new(0)));
        }
        let last_index = self.last_index[&args.name].clone();
        if args.seq < last_index.load(Ordering::SeqCst) {
            return sender
                .send(Err(Status::already_exists("Duplicated Request")))
                .unwrap_or(());
        }
        let res = self.rf.start(&args);
        if res.is_err() {
            return sender
                .send(Err(Status::permission_denied("Not Leader")))
                .unwrap_or(());
        }
        let (index, _term) = res.unwrap();
        let (tx, rx) = channel();
        self.pending.insert(index, KvEvent::Get(args, tx));

        tokio::spawn(async move {
            let reply = match timeout(Duration::from_millis(3000), rx).await {
                Ok(Ok(Ok(reply))) => {
                    last_index.store(reply.seq, Ordering::SeqCst);
                    Ok(reply)
                }
                Ok(Ok(Err(status))) => Err(status),
                Ok(Err(_e)) => Err(Status::cancelled("Recv Error")),
                Err(_e) => Err(Status::deadline_exceeded("Timeout")),
            };
            sender.send(reply).unwrap_or(());
        });
    }
    fn handle_remove(&mut self, args: RemoveRequest, sender: Sender<Result<RemoveReply, Status>>) {
        if !self.last_index.contains_key(&args.name) {
            self.last_index
                .insert(args.name.clone(), Arc::new(AtomicU64::new(0)));
        }
        let last_index = self.last_index[&args.name].clone();
        if args.seq < last_index.load(Ordering::SeqCst) {
            return sender
                .send(Err(Status::already_exists("Duplicated Request")))
                .unwrap_or(());
        }
        let res = self.rf.start(&args);
        if res.is_err() {
            return sender
                .send(Err(Status::permission_denied("Not Leader")))
                .unwrap_or(());
        }
        let (index, _term) = res.unwrap();
        let (tx, rx) = channel();
        self.pending.insert(index, KvEvent::Remove(args, tx));

        tokio::spawn(async move {
            let reply = match timeout(Duration::from_millis(3000), rx).await {
                Ok(Ok(Ok(reply))) => {
                    last_index.store(reply.seq, Ordering::SeqCst);
                    Ok(reply)
                }
                Ok(Ok(Err(status))) => Err(status),
                Ok(Err(_e)) => Err(Status::cancelled("Recv Error")),
                Err(_e) => Err(Status::deadline_exceeded("Timeout")),
            };
            sender.send(reply).unwrap_or(());
        });
    }

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
        let index = msg.command_index;
        if let Ok(req) = SetRequest::decode(&*msg.command) {
            if !self.last_index.contains_key(&req.name) {
                self.last_index
                    .insert(req.name.clone(), Arc::new(AtomicU64::new(0)));
            }
            info!("q");
            if let Some(KvEvent::Set(args, tx)) = self.pending.remove(&index) {
                info!("w");
                if req.seq == args.seq
                    && req.seq > self.last_index[&req.name].load(Ordering::SeqCst)
                {
                    info!("get set apply msg: {}, {}", args.key, args.value);
                    self.last_index[&req.name].store(req.seq, Ordering::SeqCst);
                    let reply = self
                        .store
                        .set(req.key, req.value)
                        .map(|_| SetReply {
                            message: "OK".to_owned(),
                            name: args.name,
                            seq: args.seq,
                        })
                        .map_err(|e| Status::internal(e.to_string()));
                    return tx.send(reply).unwrap_or(());
                }
            }
        }
        if let Ok(req) = GetRequest::decode(&*msg.command) {
            if !self.last_index.contains_key(&req.name) {
                self.last_index
                    .insert(req.name.clone(), Arc::new(AtomicU64::new(0)));
            }
            if let Some(KvEvent::Get(args, tx)) = self.pending.remove(&index) {
                if req.seq == args.seq
                    && req.seq > self.last_index[&req.name].load(Ordering::SeqCst)
                {
                    self.last_index[&req.name].store(req.seq, Ordering::SeqCst);
                    let reply = self
                        .store
                        .get(req.key)
                        .map(|value| GetReply {
                            message: value.unwrap_or(String::from("Key not found")),
                            name: args.name,
                            seq: args.seq,
                        })
                        .map_err(|e| Status::internal(e.to_string()));
                    tx.send(reply).unwrap_or(());
                }
            }
        }
        if let Ok(req) = RemoveRequest::decode(&*msg.command) {
            if !self.last_index.contains_key(&req.name) {
                self.last_index
                    .insert(req.name.clone(), Arc::new(AtomicU64::new(0)));
            }
            if let Some(KvEvent::Remove(args, tx)) = self.pending.remove(&index) {
                if req.seq == args.seq
                    && req.seq > self.last_index[&req.name].load(Ordering::SeqCst)
                {
                    self.last_index[&req.name].store(req.seq, Ordering::SeqCst);
                    let reply = self
                        .store
                        .remove(req.key)
                        .map(|_| RemoveReply {
                            message: String::from("OK"),
                            name: args.name,
                            seq: args.seq,
                        })
                        .map_err(|e| Status::internal(e.to_string()));
                    tx.send(reply).unwrap_or(());
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum KvEvent {
    Set(SetRequest, Sender<Result<SetReply, Status>>),
    Get(GetRequest, Sender<Result<GetReply, Status>>),
    Remove(RemoveRequest, Sender<Result<RemoveReply, Status>>),
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

#[derive(Clone)]
pub struct KvRaftNode {
    handle: Arc<Mutex<thread::JoinHandle<()>>>,
    sender: UnboundedSender<KvEvent>,
}

impl KvRaftNode {
    pub fn new(
        rf: raft::RaftNode,
        store: EngineKind,
        me: usize,
        persister: Arc<dyn persister::Persister>,
        maxraftstate: Option<usize>,
        apply_ch: UnboundedReceiver<ApplyMsg>,
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
        }
    }
}

#[tonic::async_trait]
impl KvRpc for KvRaftNode {
    async fn set(
        &self,
        req: Request<SetRequest>,
    ) -> std::result::Result<Response<SetReply>, Status> {
        let req = req.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::Set(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|e| Err(Status::cancelled(e.to_string())))
            .map(|reply| Response::new(reply))
    }
    async fn get(
        &self,
        req: Request<GetRequest>,
    ) -> std::result::Result<Response<GetReply>, Status> {
        let req = req.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::Get(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|e| Err(Status::cancelled(e.to_string())))
            .map(|reply| Response::new(reply))
    }
    async fn remove(
        &self,
        req: Request<RemoveRequest>,
    ) -> std::result::Result<Response<RemoveReply>, Status> {
        let req = req.into_inner();
        let (tx, rx) = channel();
        self.sender.send(KvEvent::Remove(req, tx)).unwrap();
        rx.await
            .unwrap_or_else(|e| Err(Status::cancelled(e.to_string())))
            .map(|reply| Response::new(reply))
    }
}
