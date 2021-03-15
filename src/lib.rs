// #![deny(missing_docs)]
//! A key-value store system

#![allow(dead_code)]
// #![allow(unused)]

#[macro_use]
extern crate log;

mod backend;
mod client;
mod common;
mod config;
mod error;
mod raft;
mod rpc;
mod server;
/// Thread Pool
pub mod thread_pool;

pub use backend::{EngineKind, KvSled, KvStore, KvsEngine};
pub use client::KvsClient;
pub use common::{Request, Response};
pub use error::{KvError, Result};
pub use rpc::kvs_service::*;
pub use server::{KvsServer, KvsServerBuilder};
// pub use thread_pool::{NaiveThreadPool, RayonThreadPool, ShareQueueThreadPool, ThreadPool};
// pub use thread_pool;
pub use raft::{FilePersister, KvRaftNode, Persister, RaftConfig, RaftNode};

/// preclude
pub mod preclude {
    pub use crate::backend::{EngineKind, KvSled, KvStore, KvsEngine};
    pub use crate::client::KvsClient;
    // pub use crate::common::{Request, Response};
    pub use crate::error::{KvError, Result};
    pub use crate::raft::{FilePersister, KvRaftNode, Persister, RaftNode};
    pub use crate::rpc::kvs_service::*;
    pub use crate::rpc::raft_service::*;
    pub use crate::server::{KvsServer, KvsServerBuilder};
}
