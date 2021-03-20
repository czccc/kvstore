use std::{ops::Bound::*, ops::RangeBounds, path::PathBuf, str::FromStr};

use super::*;
use crate::preclude::*;
/// A three column data store including Data, Lock, Write
pub struct MultiStore {
    data: EngineKind,
    lock: EngineKind,
    write: EngineKind,
}

impl MultiStore {
    /// Create a new MultiStore in given path
    pub fn new(path: impl Into<PathBuf>, store_kind: String) -> Self {
        let path: PathBuf = path.into();
        let data = match store_kind.as_ref() {
            "kvs" => EngineKind::kvs(KvStore::open(path.join("data")).unwrap()),
            "sled" => EngineKind::sled(KvSled::open(path.join("data")).unwrap()),
            _unknown => unreachable!(),
        };
        let lock = match store_kind.as_ref() {
            "kvs" => EngineKind::kvs(KvStore::open(path.join("lock")).unwrap()),
            "sled" => EngineKind::sled(KvSled::open(path.join("lock")).unwrap()),
            _unknown => unreachable!(),
        };
        let write = match store_kind.as_ref() {
            "kvs" => EngineKind::kvs(KvStore::open(path.join("write")).unwrap()),
            "sled" => EngineKind::sled(KvSled::open(path.join("write")).unwrap()),
            _unknown => unreachable!(),
        };
        MultiStore { data, lock, write }
    }
    /// Reads the latest key-value record from a Data column
    /// in MemoryStorage with a given key and a timestamp range.
    #[inline]
    pub fn read_data(
        &self,
        key: String,
        ts_start: Option<u64>,
        ts_end: Option<u64>,
    ) -> Option<(Key, DataValue)> {
        let range = generate_range(key, ts_start, ts_end);
        self.data.range_last(range).unwrap().map(|(key, value)| {
            (
                Key::from_str(&key).unwrap(),
                DataValue::from_str(&value).unwrap(),
            )
        })
    }
    /// Reads the latest key-value record from a specified column
    /// in MemoryStorage with a given key and a timestamp range.
    #[inline]
    pub fn read_lock(
        &self,
        key: String,
        ts_start: Option<u64>,
        ts_end: Option<u64>,
    ) -> Option<(Key, LockValue)> {
        let range = generate_range(key, ts_start, ts_end);
        self.lock.range_last(range).unwrap().map(|(key, value)| {
            (
                Key::from_str(&key).unwrap(),
                LockValue::from_str(&value).unwrap(),
            )
        })
    }
    /// Reads the latest key-value record from a specified column
    /// in MemoryStorage with a given key and a timestamp range.
    #[inline]
    pub fn read_write(
        &self,
        key: String,
        ts_start: Option<u64>,
        ts_end: Option<u64>,
    ) -> Option<(Key, WriteValue)> {
        let range = generate_range(key, ts_start, ts_end);
        self.write.range_last(range).unwrap().map(|(key, value)| {
            (
                Key::from_str(&key).unwrap(),
                WriteValue::from_str(&value).unwrap(),
            )
        })
    }

    /// Writes a record to a specified column in MemoryStorage.
    #[inline]
    pub fn write_data(&self, key: String, ts: u64, value: String) {
        let key = Key::new(key, ts);
        let value = DataValue::new(value);
        self.data.set(key.to_string(), value.to_string()).unwrap();
    }
    /// Writes a record to a specified column in MemoryStorage.
    #[inline]
    pub fn write_lock(&self, key: String, ts: u64, primary: String, op: WriteOp) {
        let key = Key::new(key, ts);
        let value = LockValue::new(primary, op);
        self.lock.set(key.to_string(), value.to_string()).unwrap();
    }
    /// Writes a record to a specified column in MemoryStorage.
    #[inline]
    pub fn update_lock(&self, primary: String, ts: u64) {
        match self.read_lock(primary, Some(ts), Some(ts)) {
            Some((lock_key, lock_value)) => {
                let new_value = LockValue::new(lock_value.primary(), lock_value.op());
                self.lock
                    .set(lock_key.to_string(), new_value.to_string())
                    .unwrap();
            }
            None => {}
        }
    }
    /// Writes a record to a specified column in MemoryStorage.
    #[inline]
    pub fn write_write(&self, key: String, ts: u64, value: u64, op: WriteOp) {
        let key = Key::new(key, ts);
        let value = WriteValue::new(value, op);
        self.write.set(key.to_string(), value.to_string()).unwrap();
    }

    #[inline]
    /// Erases a record from a specified column in MemoryStorage.
    pub fn erase_data(&self, key: String, commit_ts: u64) {
        let range = generate_range(key, None, Some(commit_ts));
        self.data.range_erase(range).unwrap();
    }
    #[inline]
    /// Erases a record from a specified column in MemoryStorage.
    pub fn erase_lock(&self, key: String, commit_ts: u64) {
        let range = generate_range(key, None, Some(commit_ts));
        self.lock.range_erase(range).unwrap();
    }
    #[inline]
    /// Erases a record from a specified column in MemoryStorage.
    pub fn erase_write(&self, key: String, commit_ts: u64) {
        let range = generate_range(key, None, Some(commit_ts));
        self.write.range_erase(range).unwrap();
    }
}

impl MultiStore {
    pub fn export(&self) -> Result<Vec<Vec<String>>> {
        let mut res = Vec::new();
        let data = self.data.export()?;
        res.push(data.0);
        res.push(data.1);
        let data = self.lock.export()?;
        res.push(data.0);
        res.push(data.1);
        let data = self.write.export()?;
        res.push(data.0);
        res.push(data.1);
        Ok(res)
    }
    pub fn import(&self, mut data: Vec<Vec<String>>) -> Result<()> {
        let value = data.pop().unwrap();
        let key = data.pop().unwrap();
        self.write.import((key, value))?;
        let value = data.pop().unwrap();
        let key = data.pop().unwrap();
        self.lock.import((key, value))?;
        let value = data.pop().unwrap();
        let key = data.pop().unwrap();
        self.data.import((key, value))?;
        Ok(())
    }
}

fn generate_key(key: &str, ts: u64) -> String {
    Key::new(key.to_string(), ts).to_string()
}

fn generate_range(key: String, start: Option<u64>, end: Option<u64>) -> impl RangeBounds<String> {
    let key_start = start.map_or(Included(generate_key(&key, u64::MIN)), |v| {
        Included(generate_key(&key, v))
    });
    let key_end = end.map_or(Included(generate_key(&key, u64::MAX)), |v| {
        Included(generate_key(&key, v))
    });
    (key_start, key_end)
}
