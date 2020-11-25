use std::{path::PathBuf, str::from_utf8};

use crate::{KvError, KvsEngine, Result};

/// Key-Value Store in sled
#[derive(Debug)]
pub struct KvSled {
    db: sled::Db,
}

impl KvSled {
    /// open
    pub fn open(path: impl Into<PathBuf>) -> Result<impl KvsEngine> {
        let db: sled::Db = sled::open(path.into()).unwrap();
        // if db.was_recovered() {
        //     println!("Recovered from previous!");
        // } else {
        //     println!("Create new data store!");
        // }
        Ok(KvSled { db })
    }
}

impl KvsEngine for KvSled {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        match self.db.insert(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                self.db.flush().unwrap();
                Ok(())
            }
            Err(_) => Err(KvError::Unknown),
        }
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.db.get(key.as_bytes()) {
            Ok(Some(value)) => Ok(Some(
                from_utf8(value.to_vec().as_ref()).unwrap().to_string(),
            )),
            Ok(None) => Ok(None),
            Err(_) => Err(KvError::Unknown),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.db.remove(key) {
            Ok(Some(_)) => {
                self.db.flush().unwrap();
                Ok(())
            }
            Ok(None) => Err(KvError::KeyNotFound),
            Err(_) => Err(KvError::Unknown),
        }
    }
}
