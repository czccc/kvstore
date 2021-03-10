use crate::thread_pool::ThreadPool;

/// RayonThreadPool
#[derive(Debug)]
pub struct RayonThreadPool {
    rayon: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            rayon: rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build()
                .unwrap(),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.rayon.spawn(job);
    }
}
