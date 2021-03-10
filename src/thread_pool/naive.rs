use std::thread;

use crate::thread_pool::ThreadPool;

/// NaiveThreadPool
#[derive(Debug)]
pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
