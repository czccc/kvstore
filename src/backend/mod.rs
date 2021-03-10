use crate::Result;

pub use kvsled::KvSled;
pub use kvstore::KvStore;

mod kvsled;
mod kvstore;

/// The KvsEngine trait supports the following methods:
// pub trait KvsBackend: KvsEngine + Clone + Send + 'static {}

/// The KvsEngine trait supports the following methods:
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the value of a string key to a string.
    ///
    /// Return an error if the value is not written successfully.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get the string value of a string key. If the key does not exist, return None.
    ///
    ///Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>>;
    ///Remove a given string key.
    ///
    ///Return an error if the key does not exit or value is not read successfully.
    fn remove(&self, key: String) -> Result<()>;
}

/// kind
#[allow(non_camel_case_types)]
#[derive(Debug, Clone)]
pub enum EngineKind {
    /// kvs
    kvs(KvStore),
    /// sled
    sled(KvSled),
}

impl EngineKind {
    /// a
    pub fn set(&self, key: String, value: String) -> Result<()> {
        match self {
            EngineKind::kvs(store) => store.set(key, value),
            EngineKind::sled(store) => store.set(key, value),
        }
    }
    /// a
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self {
            EngineKind::kvs(store) => store.get(key),
            EngineKind::sled(store) => store.get(key),
        }
    }
    /// a
    pub fn remove(&self, key: String) -> Result<()> {
        match self {
            EngineKind::kvs(store) => store.remove(key),
            EngineKind::sled(store) => store.remove(key),
        }
    }
}
