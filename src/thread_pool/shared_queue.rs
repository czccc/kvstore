use crate::thread_pool::ThreadPool;

/// ShareQueueThreadPool
pub struct SharedQueueThreadPool {}

impl ThreadPool for SharedQueueThreadPool {
    fn new(_threads: u32) -> crate::Result<Self>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn spawn<F>(&self, _job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        unimplemented!()
    }
}
