use crate::{KvError, Result};
use std::str::FromStr;

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

/// TimeStamp Oracle used to generate unique timestamp
pub trait TimeStampOracle {
    /// Generate Transaction ts
    fn next_timestamp(&self) -> Result<u64>;
}

/// Backend Engine
#[allow(non_camel_case_types)]
#[derive(Debug)]
pub enum Engine {
    /// Use log-structure engine
    kvs,
    /// Use sled engine
    sled,
}

// impl Engine {
//     /// Open a backend Engine in given path
//     pub fn open<E: KvsEngine>(engine: Engine, path: impl Into<PathBuf>) -> Result<E> {
//         match engine {
//             Engine::kvs => Ok(KvStore::open(path.into())?),
//             // Engine::kvs => Ok(Box::new(KvStore::open(path.into())?)),
//             Engine::sled => Ok(Box::new(KvSled::open(path.into())?)),
//         }
//     }
// }

impl FromStr for Engine {
    type Err = KvError;

    fn from_str(s: &str) -> Result<Self> {
        if s == "kvs" {
            Ok(Engine::kvs)
        } else if s == "sled" {
            Ok(Engine::sled)
        } else {
            Err(KvError::ParserError(s.to_string()))
        }
    }
}
