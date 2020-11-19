#![deny(missing_docs)]
//! A key-value store system
pub use error::KvError;
pub use error::Result;
pub use kv::KvStore;

mod error;
mod kv;
