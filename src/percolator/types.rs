use chrono::prelude::*;
use std::{
    fmt::Display,
    str::FromStr,
    time::{Duration, Instant, SystemTime},
};

pub enum Column {
    Write,
    Data,
    Lock,
}

/// A Key struct used in percolator txn
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
pub struct DataValue {
    value: String,
}

impl DataValue {
    /// Create a new DataValue
    pub fn new(value: String) -> Self {
        Self { value }
    }
    /// Get the inner value
    pub fn value(&self) -> &str {
        self.value.as_ref()
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

/// A LockValue struct
pub struct LockValue {
    primary: String,
    ttl: DateTime<Utc>,
}

impl LockValue {
    /// Create a new LockValue struct
    pub fn new(primary: String, ttl: Instant) -> Self {
        let system_now = SystemTime::now();
        let instant_now = Instant::now();
        let approx = system_now - (instant_now - ttl);
        Self {
            primary,
            ttl: approx.into(),
        }
    }
    /// Get the string value of primary
    pub fn primary(&self) -> &str {
        self.primary.as_ref()
    }
    /// Compute how long this key is elapsed
    pub fn elapsed(&self) -> Duration {
        let system_now = SystemTime::now();
        let ttl: SystemTime = self.ttl.into();
        system_now.duration_since(ttl).expect("Time backward!")
    }
}

impl Display for LockValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}~{}", self.primary, self.ttl)
    }
}

impl FromStr for LockValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splited = s.rsplit_terminator("~");
        let ttl = splited.next().unwrap().parse().unwrap();
        let primary = splited.collect();
        Ok(LockValue { primary, ttl })
    }
}

/// A WriteValue struct
pub struct WriteValue {
    ts: u64,
}

impl WriteValue {
    /// Create a new WriteValue
    pub fn new(ts: u64) -> Self {
        Self { ts }
    }
    /// Get the value of ts
    pub fn ts(&self) -> u64 {
        self.ts
    }
}

impl Display for WriteValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.ts)
    }
}

impl FromStr for WriteValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(WriteValue {
            ts: s.parse().unwrap(),
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
        let now = Instant::now();
        let value = LockValue::new(String::from("some value"), now);
        let ss = value.to_string();
        println!("{}", ss);
        let new_value = LockValue::from_str(&ss).unwrap();
        println!("{}", new_value);
        sleep(Duration::from_secs(1));
        println!("{:?}", new_value.elapsed());
    }
}
