use crate::thread_pool::ThreadPool;
use crossbeam::channel::{unbounded, Receiver, Sender};
use std::thread;

/// ShareQueueThreadPool
pub struct SharedQueueThreadPool {
    sender: Sender<ThreadPoolMessage>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(size: u32) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = unbounded();

        for id in 0..size {
            let receiver = receiver.clone();
            let worker = Worker::new(id, receiver);
            thread::spawn(move || run(worker));
        }
        Ok(SharedQueueThreadPool { sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .expect("Unable to send");
    }
}

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
}

struct Worker {
    id: u32,
    receiver: Receiver<ThreadPoolMessage>,
}

impl Worker {
    fn new(id: u32, receiver: Receiver<ThreadPoolMessage>) -> Self {
        Worker { id, receiver }
    }
}

fn run(worker: Worker) {
    loop {
        let message = worker.receiver.recv();
        match message {
            Ok(ThreadPoolMessage::RunJob(job)) => {
                job();
            }
            // Ok(ThreadPoolMessage::Shutdown) => {
            //     println!("Receive Shutdown!");
            //     break;
            // }
            Err(_) => {
                // println!("{}", e);
                break;
            }
        };
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            // println!("id: {} dropped while unwinding", self.id);
            let receiver = self.receiver.clone();
            let worker = Worker::new(self.id, receiver);
            thread::spawn(move || run(worker));
        }
    }
}
