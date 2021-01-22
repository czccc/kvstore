#![deny(missing_docs)]
//! A key-value store system

#[macro_use]
extern crate log;

mod backend;
mod client;
mod common;
mod error;
mod server;
/// Thread Pool mod with three implements:
/// NaiveThreadPool, RayonThreadPool, ShareQueueThreadPool
pub mod thread_pool;

pub use backend::{Engine, KvSled, KvStore, KvsBackend, KvsEngine, TimeStampOracle};
pub use client::KvsClient;
pub use common::{Request, Response};
pub use error::{KvError, Result};
pub use server::KvsServer;

/// Default Engine tag file
pub const ENGINE_TAG_FILE: &str = ".engine";
