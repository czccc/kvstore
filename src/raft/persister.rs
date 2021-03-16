//! Support for Raft and kvraft to save persistent
//! Raft state (log &c) and k/v server snapshots.
//!
//! we will use the original persister.rs to test your code for grading.
//! so, while you can modify this code to help you debug, please
//! test with the original before submitting.

use std::{
    fs::OpenOptions,
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Persister defined how raft state and snapshot can persist to disk
pub trait Persister: Send + Sync + 'static {
    /// get the persisted state
    fn raft_state(&self) -> Vec<u8>;
    /// save the state to disk
    fn save_raft_state(&self, state: Vec<u8>);
    /// save the state and snapshot to disk
    fn save_state_and_snapshot(&self, state: Vec<u8>, snapshot: Vec<u8>);
    /// get the persisted snapshot
    fn snapshot(&self) -> Vec<u8>;
}

impl<T: ?Sized + Persister> Persister for Box<T> {
    fn raft_state(&self) -> Vec<u8> {
        (**self).raft_state()
    }
    fn save_raft_state(&self, state: Vec<u8>) {
        (**self).save_raft_state(state)
    }
    fn save_state_and_snapshot(&self, state: Vec<u8>, snapshot: Vec<u8>) {
        (**self).save_state_and_snapshot(state, snapshot)
    }
    fn snapshot(&self) -> Vec<u8> {
        (**self).snapshot()
    }
}

impl<T: ?Sized + Sync + Persister> Persister for Arc<T> {
    fn raft_state(&self) -> Vec<u8> {
        (**self).raft_state()
    }
    fn save_raft_state(&self, state: Vec<u8>) {
        (**self).save_raft_state(state)
    }
    fn save_state_and_snapshot(&self, state: Vec<u8>, snapshot: Vec<u8>) {
        (**self).save_state_and_snapshot(state, snapshot)
    }
    fn snapshot(&self) -> Vec<u8> {
        (**self).snapshot()
    }
}

/// FilePersister is a raft persister that save all data to files
pub struct FilePersister {
    raft_state: PathBuf,
    snapshot: PathBuf,
}

impl Default for FilePersister {
    fn default() -> Self {
        FilePersister {
            raft_state: PathBuf::from("raft_state.bin"),
            snapshot: PathBuf::from("snapshot.bin"),
        }
    }
}

impl FilePersister {
    /// Create a new FilePersister
    pub fn new() -> Self {
        Self::default()
    }
    /// Create a new FilePersister with given path
    pub fn with_path(path: PathBuf) -> Self {
        std::fs::create_dir(path.clone()).unwrap_or(());
        let mut per = Self::default();
        per.raft_state = path.clone().join(per.raft_state);
        per.snapshot = path.clone().join(per.snapshot);
        per
    }
}

impl Persister for FilePersister {
    fn raft_state(&self) -> Vec<u8> {
        let mut reader = BufReader::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(self.raft_state.clone())
                .unwrap(),
        );
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        buf
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut reader = BufReader::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(self.snapshot.clone())
                .unwrap(),
        );
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        buf
    }

    fn save_raft_state(&self, state: Vec<u8>) {
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(self.raft_state.clone())
                .unwrap(),
        );
        writer.write_all(&state).unwrap();
        writer.flush().unwrap();
    }

    fn save_state_and_snapshot(&self, state: Vec<u8>, snapshot: Vec<u8>) {
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(self.raft_state.clone())
                .unwrap(),
        );
        writer.write_all(&state).unwrap();
        writer.flush().unwrap();
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(self.snapshot.clone())
                .unwrap(),
        );
        writer.write_all(&snapshot).unwrap();
        writer.flush().unwrap();
    }
}

#[derive(Default)]
pub struct SimplePersister {
    states: Mutex<(
        Vec<u8>, // raft state
        Vec<u8>, // snapshot
    )>,
}

impl SimplePersister {
    pub fn new() -> SimplePersister {
        SimplePersister {
            states: Mutex::default(),
        }
    }
}

impl Persister for SimplePersister {
    fn raft_state(&self) -> Vec<u8> {
        self.states.lock().unwrap().0.clone()
    }

    fn save_raft_state(&self, state: Vec<u8>) {
        self.states.lock().unwrap().0 = state;
    }

    fn save_state_and_snapshot(&self, state: Vec<u8>, snapshot: Vec<u8>) {
        self.states.lock().unwrap().0 = state;
        self.states.lock().unwrap().1 = snapshot;
    }

    fn snapshot(&self) -> Vec<u8> {
        self.states.lock().unwrap().1.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_safety() {
        let sp = SimplePersister::new();
        sp.save_raft_state(vec![111]);
        let obj: Box<dyn Persister + Sync> = Box::new(sp);
        assert_eq!(obj.raft_state(), vec![111]);
        obj.save_state_and_snapshot(vec![222], vec![123]);
        assert_eq!(obj.raft_state(), vec![222]);
        assert_eq!(obj.snapshot(), vec![123]);

        let cloneable_obj: Arc<dyn Persister> = Arc::new(obj);
        assert_eq!(cloneable_obj.raft_state(), vec![222]);
        assert_eq!(cloneable_obj.snapshot(), vec![123]);

        let cloneable_obj_ = cloneable_obj.clone();
        cloneable_obj.save_raft_state(vec![233]);
        assert_eq!(cloneable_obj_.raft_state(), vec![233]);
        assert_eq!(cloneable_obj_.snapshot(), vec![123]);

        let sp = SimplePersister::new();
        let obj: Arc<dyn Persister + Sync> = Arc::new(sp);
        let _box_obj: Box<dyn Persister> = Box::new(obj);
    }
}
