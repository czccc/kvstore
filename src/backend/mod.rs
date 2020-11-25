use crate::{KvError, Result};
use std::{path::PathBuf, str::FromStr};

pub use kvsled::KvSled;
pub use kvstore::KvStore;

mod kvsled;
mod kvstore;

/// The KvsEngine trait supports the following methods:
pub trait KvsEngine {
    /// Set the value of a string key to a string.
    ///
    /// Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Get the string value of a string key. If the key does not exist, return None.
    ///
    ///Return an error if the value is not read successfully.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    ///Remove a given string key.
    ///
    ///Return an error if the key does not exit or value is not read successfully.
    fn remove(&mut self, key: String) -> Result<()>;
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

impl Engine {
    /// Open kvstore in given path
    pub fn open(&self, path: impl Into<PathBuf>) -> Result<Box<dyn KvsEngine>> {
        match self {
            Engine::kvs => Ok(Box::new(KvStore::open(path.into())?)),
            Engine::sled => Ok(Box::new(KvSled::open(path.into())?)),
        }
    }
}

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
