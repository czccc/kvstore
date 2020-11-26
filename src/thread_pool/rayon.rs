use crate::thread_pool::ThreadPool;

/// RayonThreadPool
pub struct RayonThreadPool {}

impl ThreadPool for RayonThreadPool {
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
