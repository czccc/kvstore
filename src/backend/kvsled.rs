use std::{path::PathBuf, str::from_utf8};

use crate::*;

/// Key-Value Store, implement in sled
#[derive(Debug, Clone)]
pub struct KvSled {
    db: sled::Db,
}

impl KvSled {
    /// Open KvSled at given path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvSled> {
        let db: sled::Db = sled::open(path.into()).unwrap();
        Ok(KvSled { db })
    }
}

impl KvsEngine for KvSled {
    fn set(&self, key: String, value: String) -> Result<()> {
        match self.db.insert(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                self.db.flush().unwrap();
                Ok(())
            }
            Err(_) => Err(KvError::Unknown),
        }
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.db.get(key.as_bytes()) {
            Ok(Some(value)) => Ok(Some(
                from_utf8(value.to_vec().as_ref()).unwrap().to_string(),
            )),
            Ok(None) => Ok(None),
            Err(_) => Err(KvError::Unknown),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        match self.db.remove(key) {
            Ok(Some(_)) => {
                self.db.flush().unwrap();
                Ok(())
            }
            Ok(None) => Err(KvError::KeyNotFound),
            Err(_) => Err(KvError::Unknown),
        }
    }

    fn export(&self) -> Result<(Vec<String>, Vec<String>)> {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        self.db
            .iter()
            .map(|entity| {
                let (key, value) = entity.unwrap();
                keys.push(String::from_utf8(key.to_vec()).unwrap());
                values.push(String::from_utf8(value.to_vec()).unwrap());
            })
            .for_each(drop);
        Ok((keys, values))
    }
    fn import(&self, data: (Vec<String>, Vec<String>)) -> Result<()> {
        self.db.clear().unwrap();
        let (keys, values) = data;
        keys.into_iter()
            .zip(values.into_iter())
            .map(|(key, value)| self.db.insert(key.as_bytes(), value.as_bytes()))
            .for_each(drop);
        self.db.flush().unwrap();
        Ok(())
    }
}
