// #![deny(missing_docs)]
//! A key-value store system

// #[macro_use]
// extern crate log;

mod backend;
mod client;
mod common;
mod error;
/// a
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

/// preclude
pub mod preclude {
    pub use crate::backend::{EngineKind, KvSled, KvStore, KvsEngine};
    pub use crate::client::KvsClient;
    pub use crate::common::{Request, Response};
    pub use crate::error::{KvError, Result};
    pub use crate::rpc::kvs_service::*;
    pub use crate::server::{KvsServer, KvsServerBuilder};
}
