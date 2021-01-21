use std::{
    fs,
    io::Write,
    path::PathBuf,
    str::from_utf8,
    sync::{atomic::AtomicU64, Arc},
};

use crate::*;

/// Key-Value Store, implement in sled
#[derive(Debug, Clone)]
pub struct KvSled {
    db: sled::Db,
    tso: Arc<AtomicU64>,
}

impl KvSled {
    /// Open KvSled at given path
    pub fn open(path: impl Into<PathBuf>) -> Result<KvSled> {
        let db: sled::Db = sled::open(path.into()).unwrap();

        let tso: u64 = fs::read_to_string(".tso")
            .unwrap_or("0".to_string())
            .parse()
            .expect("could parse string to u64");

        Ok(KvSled {
            db,
            tso: Arc::new(AtomicU64::new(tso + 1)),
        })
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
}

impl TimeStampOracle for KvSled {
    fn next_timestamp(&self) -> Result<u64> {
        let ts = self.tso.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut tso_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(".tso")?;
        tso_file.write(ts.to_string().as_bytes())?;
        tso_file.flush()?;
        Ok(ts)
    }
}
