// use crate::rpc::KvStoreService;
use crate::backend::EngineKind;
use crate::*;
use std::{env::current_dir, path::PathBuf};
/// builder
pub struct KvsServerBuilder {
    path: PathBuf,
    engine: String,
}

impl Default for KvsServerBuilder {
    fn default() -> Self {
        KvsServerBuilder {
            path: current_dir().unwrap(),
            engine: String::from("kvs"),
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
    pub fn build(self) -> Result<KvsServer> {
        let store = match self.engine.as_ref() {
            "kvs" => EngineKind::kvs(KvStore::open(self.path)?),
            "sled" => EngineKind::sled(KvSled::open(self.path)?),
            unknown => Err(KvError::ParserError(unknown.to_string()))?,
        };
        Ok(KvsServer::new(store)?)
    }
}
