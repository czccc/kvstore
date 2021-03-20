use crate::rpc::kvs_service::WriteOp;
use chrono::prelude::*;
use std::{
    fmt::Display,
    str::FromStr,
    time::{Duration, SystemTime},
};

pub enum Column {
    Write,
    Data,
    Lock,
}

/// A Key struct used in percolator txn
#[derive(Clone)]
pub struct Key {
    key: String,
    ts: u64,
}

impl Key {
    /// Create a new Key
    pub fn new(key: String, ts: u64) -> Self {
        Self { key, ts }
    }
    /// Get the string value of inner key
    pub fn key(&self) -> &str {
        self.key.as_ref()
    }
    /// Get the value of ts
    pub fn ts(&self) -> u64 {
        self.ts
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{:020}", self.key, self.ts)
    }
}

impl FromStr for Key {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splited = s.rsplit_terminator("-");
        let ts = splited.next().unwrap().parse().unwrap();
        let key = splited.collect();
        Ok(Key { key, ts })
    }
}

/// A DataValue struct
#[derive(Clone)]
pub struct DataValue {
    value: String,
}

impl DataValue {
    /// Create a new DataValue
    pub fn new(value: String) -> Self {
        Self { value }
    }
    /// Get the inner value
    pub fn value(self) -> String {
        self.value
    }
}

impl Display for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl FromStr for DataValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(DataValue {
            value: s.to_string(),
        })
    }
}

enum LockType {
    PreWrite,
    Get,
    RollBack,
}

/// A LockValue struct
#[derive(Clone)]
pub struct LockValue {
    primary: String,
    ttl: DateTime<Utc>,
    op: WriteOp,
}

impl LockValue {
    /// Create a new LockValue struct
    pub fn new(primary: String, op: WriteOp) -> Self {
        Self {
            primary,
            ttl: SystemTime::now().into(),
            op,
        }
    }
    /// Get the string value of primary
    pub fn primary(&self) -> String {
        self.primary.clone()
    }
    /// Get the string value of primary
    pub fn op(&self) -> WriteOp {
        self.op
    }
    /// Compute how long this key is elapsed
    pub fn elapsed(&self) -> Duration {
        let system_now = SystemTime::now();
        let ttl: SystemTime = self.ttl.into();
        system_now.duration_since(ttl).expect("Time backward!")
    }
    /// reset ttl
    pub fn reset_ttl(&mut self) {
        self.ttl = SystemTime::now().into();
    }
}

impl Display for LockValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}~{}~{}", self.primary, self.ttl, self.op)
    }
}

impl FromStr for LockValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splited = s.rsplit_terminator("~");
        let op = splited.next().unwrap().parse().unwrap();
        let ttl = splited.next().unwrap().parse().unwrap();
        let primary = splited.collect();
        Ok(LockValue { primary, ttl, op })
    }
}

impl Display for WriteOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteOp::Put => write!(f, "Put"),
            WriteOp::Lock => write!(f, "Lock"),
            WriteOp::Delete => write!(f, "Delete"),
        }
    }
}

impl FromStr for WriteOp {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Put" => Ok(WriteOp::Put),
            "Lock" => Ok(WriteOp::Lock),
            "Delete" => Ok(WriteOp::Delete),
            _ => Err(()),
        }
    }
}
/// A WriteValue struct
#[derive(Clone)]
pub struct WriteValue {
    ts: u64,
    op: WriteOp,
}

impl WriteValue {
    /// Create a new WriteValue
    pub fn new(ts: u64, op: WriteOp) -> Self {
        Self { ts, op }
    }
    /// Get the value of ts
    pub fn ts(&self) -> u64 {
        self.ts
    }
    /// Get the value of write op
    pub fn op(&self) -> WriteOp {
        self.op
    }
}

impl Display for WriteValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ts, self.op)
    }
}

impl FromStr for WriteValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splited = s.split_terminator("-");
        let ts = splited.next().unwrap();
        let op = splited.next().unwrap();
        Ok(WriteValue {
            ts: ts.parse().unwrap(),
            op: op.parse().unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use super::*;
    #[test]
    fn test_lock_value() {
        assert_eq!(2, 1 + 1);
        let value = LockValue::new(String::from("some value"), WriteOp::Put);
        let ss = value.to_string();
        println!("{}", ss);
        let new_value = LockValue::from_str(&ss).unwrap();
        println!("{}", new_value);
        sleep(Duration::from_secs(1));
        println!("{:?}", new_value.elapsed());
    }
}
