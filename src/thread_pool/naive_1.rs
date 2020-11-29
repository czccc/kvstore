use std::{
    sync::{mpsc, Arc, Mutex},
    thread::{self, JoinHandle},
};

use crate::thread_pool::ThreadPool;
use crate::{KvError, Result};

struct Worker {
    id: u32,
    handle: JoinHandle<()>,
}

impl Worker {
    fn new(id: u32, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let handle = thread::spawn(move || loop {
            let receiver = receiver.lock().unwrap();
            let job = receiver.recv().unwrap();
            job();
        });
        Self { id, handle }
    }
}

// struct Job {}
type Job = Box<dyn FnOnce() + Send + 'static>;

/// Naive
pub struct NaiveThreadPool {
    threads: Vec<Worker>,
    sender: mpsc::Sender<Job>,
}

impl ThreadPool for NaiveThreadPool {
    fn new(size: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut threads = Vec::with_capacity(size as usize);
        for i in 0..size {
            threads.push(Worker::new(i, Arc::clone(&receiver)))
        }
        Ok(Self { threads, sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(job);
        self.sender.send(job).unwrap();
    }
}
