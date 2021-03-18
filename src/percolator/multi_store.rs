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
    pub fn write_data(&mut self, key: String, ts: u64, value: DataValue) {
        let key = Key::new(key, ts);
        self.data.set(key.to_string(), value.to_string()).unwrap();
    }
    /// Writes a record to a specified column in MemoryStorage.
    #[inline]
    pub fn write_lock(&mut self, key: String, ts: u64, value: LockValue) {
        let key = Key::new(key, ts);
        self.lock.set(key.to_string(), value.to_string()).unwrap();
    }
    /// Writes a record to a specified column in MemoryStorage.
    #[inline]
    pub fn write_write(&mut self, key: String, ts: u64, value: WriteValue) {
        let key = Key::new(key, ts);
        self.write.set(key.to_string(), value.to_string()).unwrap();
    }

    #[inline]
    /// Erases a record from a specified column in MemoryStorage.
    pub fn erase_data(&mut self, key: String, commit_ts: u64) {
        let range = generate_range(key, None, Some(commit_ts));
        self.data.range_erase(range).unwrap();
    }
    #[inline]
    /// Erases a record from a specified column in MemoryStorage.
    pub fn erase_lock(&mut self, key: String, commit_ts: u64) {
        let range = generate_range(key, None, Some(commit_ts));
        self.lock.range_erase(range).unwrap();
    }
    #[inline]
    /// Erases a record from a specified column in MemoryStorage.
    pub fn erase_write(&mut self, key: String, commit_ts: u64) {
        let range = generate_range(key, None, Some(commit_ts));
        self.write.range_erase(range).unwrap();
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
