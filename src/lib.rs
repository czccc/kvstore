#![deny(missing_docs)]
//! A key-value store system
pub use backend::{Engine, KvSled, KvStore, KvsEngine};
pub use client::KvsClient;
pub use common::{Request, Response};
pub use error::{KvError, Result};
pub use server::KvsServer;

/// Default Engine tag file
pub const ENGINE_TAG_FILE: &str = ".engine";

mod backend;
mod client;
mod common;
mod error;
mod server;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
