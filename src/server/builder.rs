// use crate::rpc::KvStoreService;
use crate::*;
use crate::{backend::EngineKind, thread_pool::*};
// use futures::future::Ready;
use std::{env::current_dir, path::PathBuf};
/// builder
pub struct KvsServerBuilder {
    path: PathBuf,
    engine: String,
    thread_pool: String,
    num_threads: usize,
}

impl Default for KvsServerBuilder {
    fn default() -> Self {
        KvsServerBuilder {
            path: current_dir().unwrap(),
            engine: String::from("kvs"),
            thread_pool: String::from("rayon"),
            num_threads: num_cpus::get(),
        }
    }
}

impl KvsServerBuilder {
    /// builder
    pub fn new() -> Self {
        Self::default()
    }
    /// builder
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();
        self
    }
    /// builder
    pub fn engine(mut self, engine: String) -> Self {
        self.engine = engine;
        self
    }
    /// builder
    pub fn thread_pool(mut self, thread_pool: String) -> Self {
        self.thread_pool = thread_pool;
        self
    }
    /// builder
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }
    /// builder
    pub fn build(self) -> Result<KvsServer> {
        let store = match self.engine.as_ref() {
            "kvs" => EngineKind::kvs(KvStore::open(self.path)?),
            "sled" => EngineKind::sled(KvSled::open(self.path)?),
            unknown => Err(KvError::ParserError(unknown.to_string()))?,
        };
        let thread_pool = match self.thread_pool.as_ref() {
            "rayon" => ThreadPoolKind::Rayon(RayonThreadPool::new(self.num_threads as u32)?),
            "share" => {
                ThreadPoolKind::SharedQueue(SharedQueueThreadPool::new(self.num_threads as u32)?)
            }
            "naive" => ThreadPoolKind::Naive(NaiveThreadPool::new(self.num_threads as u32)?),
            unknown => Err(KvError::ParserError(unknown.to_string()))?,
        };
        Ok(KvsServer::new(store, thread_pool)?)
    }
}
