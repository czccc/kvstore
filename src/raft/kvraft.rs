use std::{
    collections::HashMap,
    sync::Arc,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
    task::Poll,
    thread,
    time::Duration,
};

use crate::{rpc::kvs_service::*, rpc::raft_service::*, EngineKind};
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
use tonic::transport::Channel;

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
        store: EngineKind,
        servers: Vec<RaftRpcClient<Channel>>,
        me: usize,
        persister: Box<dyn persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvRaftInner {
        let (tx, apply_ch) = unbounded_channel();
        let snapshot = persister.snapshot();
        let rf = raft::RaftNode::new(servers, me, persister, tx);

        let mut server = KvRaftInner {
            rf,
            me,
            maxraftstate,
            apply_ch,
            store,
            pending: HashMap::new(),
            last_index: HashMap::new(),
        };
        server.restore_from_snapshot(snapshot);
        server
    }

    fn create_snapshot(&self) -> Vec<u8> {
        todo!();
    }

    fn restore_from_snapshot(&mut self, snapshot: Vec<u8>) {
        todo!();
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
        // if !msg.command_valid {
        //     debug!("{} recv [Snapshot]", self);
        //     self.restore_from_snapshot(msg.command);
        //     return;
        // }
        debug!(
            "{} recv [ApplyMsg {} {}]",
            self, msg.command_valid, msg.command_index
        );
        let index = msg.command_index;
        if let Ok(req) = SetRequest::decode(&*msg.command) {
            if !self.last_index.contains_key(&req.name) {
                self.last_index
                    .insert(req.name.clone(), Arc::new(AtomicU64::new(0)));
            }
            if let Some(KvEvent::Set(args, tx)) = self.pending.remove(&index) {
                if req.seq == args.seq
                    && req.seq > self.last_index[&req.name].load(Ordering::SeqCst)
                {
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
                        .map(|value| RemoveReply {
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

struct KvExecutor {
    receiver: UnboundedReceiver<KvEvent>,
    server: KvRaftInner,
}

impl KvExecutor {
    fn new(kv: KvRaftInner, receiver: UnboundedReceiver<KvEvent>) -> KvExecutor {
        KvExecutor {
            server: kv,
            receiver,
        }
    }
}

impl Stream for KvExecutor {
    type Item = ();

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                debug!("{} Executor recv [Event]", self.server);
                return match event {
                    KvEvent::Set(args, tx) => {
                        self.server.handle_set(args, tx);
                        Poll::Ready(Some(()))
                    }
                    KvEvent::Get(args, tx) => {
                        self.server.handle_get(args, tx);
                        Poll::Ready(Some(()))
                    }
                    KvEvent::Remove(args, tx) => {
                        self.server.handle_remove(args, tx);
                        Poll::Ready(Some(()))
                    }
                };
            }
            Poll::Ready(None) => {}
            Poll::Pending => {}
        }
        match self.server.apply_ch.poll_recv(cx) {
            Poll::Ready(Some(msg)) => {
                self.server.handle_apply_msg(msg);
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
        store: EngineKind,
        servers: Vec<RaftRpcClient<Channel>>,
        me: usize,
        persister: Box<dyn persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvRaftNode {
        let kv_raft = KvRaftInner::new(store, servers, me, persister, maxraftstate);

        let (sender, receiver) = unbounded_channel();
        let mut raft_executor = KvExecutor::new(kv_raft, receiver);

        let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = thread::Builder::new()
            .name(format!("KvServerNode-{}", me))
            .spawn(move || {
                threaded_rt.block_on(async move {
                    debug!("Enter KvRaftInner main executor!");
                    while raft_executor.next().await.is_some() {
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
        self.sender.send(KvEvent::Set(req, tx));
        // rx.await
        todo!();
    }
    async fn get(
        &self,
        req: Request<GetRequest>,
    ) -> std::result::Result<Response<GetReply>, Status> {
        let req = req.into_inner();
        todo!();
        // self.store
        //     .get(req.key)
        //     .map(|value| GetReply {
        //         message: value.unwrap_or("Key not found".to_string()),
        //     })
        //     .map(|reply| Response::new(reply))
        //     .map_err(|e| Status::unknown(e.to_string()))
    }
    async fn remove(
        &self,
        req: Request<RemoveRequest>,
    ) -> std::result::Result<Response<RemoveReply>, Status> {
        let req = req.into_inner();
        todo!();
        // self.store
        //     .remove(req.key)
        //     .map(|_| RemoveReply {
        //         message: "OK".to_string(),
        //     })
        //     .map(|reply| Response::new(reply))
        //     .map_err(|e| Status::unknown(e.to_string()))
    }
}
