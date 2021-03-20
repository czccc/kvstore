use crate::Result;
use std::{
    fs,
    io::prelude::*,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

static DEFAULT_FILE_NAME: &'static str = ".tso";

/// A TimestampOracle
#[derive(Clone)]
pub struct TimestampOracle {
    inner: Arc<AtomicU64>,
    path: PathBuf,
}

impl TimestampOracle {
    /// Open a new TimestampOracle in the given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into().join(DEFAULT_FILE_NAME);
        let ts = restore(path.clone()).unwrap_or(1);
        Ok(Self {
            inner: Arc::new(AtomicU64::new(ts)),
            path,
        })
    }
    /// fetch a timestamp from oracle
    pub fn fetch_one(&self) -> Result<u64> {
        let ts = self.inner.fetch_add(1, Ordering::SeqCst);
        backup(self.path.clone(), ts + 1)?;
        Ok(ts)
    }
}

fn backup(path: PathBuf, ts: u64) -> Result<()> {
    let mut tso_file = fs::OpenOptions::new().create(true).write(true).open(path)?;
    tso_file.write(ts.to_string().as_bytes())?;
    tso_file.flush()?;
    Ok(())
}
fn restore(path: PathBuf) -> Result<u64> {
    let mut tso_file = fs::OpenOptions::new().read(true).open(path)?;
    let mut buf = String::new();
    tso_file.read_to_string(&mut buf)?;
    let ts: u64 = buf.parse().unwrap_or(1);
    Ok(ts)
}
