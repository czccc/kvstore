use std::ops::RangeBounds;

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
    /// Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove a given string key.
    ///
    /// Return an error if the key does not exit or value is not read successfully.
    fn remove(&self, key: String) -> Result<()>;
    ///Get the last value within a given string key range.
    ///
    ///Return an error if the value is not read successfully.
    fn range_last(&self, range: impl RangeBounds<String>) -> Result<Option<(String, String)>>;
    ///Erase a batch of value within a given string key range.
    ///
    ///Return an error if the value is not erase successfully.
    fn range_erase(&self, range: impl RangeBounds<String>) -> Result<()>;
    /// Export two `Vec` include all key and all value to backup KvsEngine
    fn export(&self) -> Result<(Vec<String>, Vec<String>)>;
    /// From two `Vec` include all key and all value to restore KvsEngine
    fn import(&self, data: (Vec<String>, Vec<String>)) -> Result<()>;
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

impl KvsEngine for EngineKind {
    /// a
    fn set(&self, key: String, value: String) -> Result<()> {
        match self {
            EngineKind::kvs(store) => store.set(key, value),
            EngineKind::sled(store) => store.set(key, value),
        }
    }
    /// a
    fn get(&self, key: String) -> Result<Option<String>> {
        match self {
            EngineKind::kvs(store) => store.get(key),
            EngineKind::sled(store) => store.get(key),
        }
    }
    /// a
    fn remove(&self, key: String) -> Result<()> {
        match self {
            EngineKind::kvs(store) => store.remove(key),
            EngineKind::sled(store) => store.remove(key),
        }
    }
    fn range_last(&self, range: impl RangeBounds<String>) -> Result<Option<(String, String)>> {
        match self {
            EngineKind::kvs(store) => store.range_last(range),
            EngineKind::sled(store) => store.range_last(range),
        }
    }
    fn range_erase(&self, range: impl RangeBounds<String>) -> Result<()> {
        match self {
            EngineKind::kvs(store) => store.range_erase(range),
            EngineKind::sled(store) => store.range_erase(range),
        }
    }
    fn export(&self) -> Result<(Vec<String>, Vec<String>)> {
        match self {
            EngineKind::kvs(store) => store.export(),
            EngineKind::sled(store) => store.export(),
        }
    }
    fn import(&self, data: (Vec<String>, Vec<String>)) -> Result<()> {
        match self {
            EngineKind::kvs(store) => store.import(data),
            EngineKind::sled(store) => store.import(data),
        }
    }
}
