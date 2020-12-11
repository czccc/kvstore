#[macro_use]
extern crate slog;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::{thread_pool::*, KvSled, KvStore, KvsClient, KvsServer};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::Rng;
use std::{fmt, thread};
use tempfile::TempDir;

#[derive(Debug)]
struct Para {
    engine: String,
    pool: String,
    key: Vec<String>,
    value: Vec<String>,
}

impl Para {
    fn new(engine: String, pool: String, len: usize) -> Para {
        let mut rng: StdRng = rand::SeedableRng::seed_from_u64(1);
        Para {
            engine,
            pool,
            key: random_string_with_length(&mut rng, len),
            value: random_string_with_length(&mut rng, len),
        }
    }
}

impl fmt::Display for Para {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(Engine: {}, ThreadPool: {})", self.engine, self.pool)
    }
}

const RANDOM_LENGTH: usize = 100;

fn random_string_with_length(rng: &mut StdRng, len: usize) -> Vec<String> {
    let mut ret = vec![];
    for _ in 0..len {
        ret.push(rng.sample_iter(&Alphanumeric).take(RANDOM_LENGTH).collect());
    }
    ret
}

fn write_bench(c: &mut Criterion) {
    let para = Para::new("kvs".to_string(), "SharedQueueThreadPool".to_string(), 100);
    c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvStore::open(temp_dir.into_path()).unwrap();
                let cpus = num_cpus::get();
                let pool = SharedQueueThreadPool::new(cpus as u32).unwrap();
                let mut server =
                    KvsServer::new(store, pool, slog::Logger::root(slog::Discard, o!())).unwrap();
                let handle =
                    thread::spawn(move || server.run("127.0.0.1:4000".parse().unwrap()).unwrap());
            },
            |()| {
                // let handle =
                //     thread::spawn(move || server.run("127.0.0.1:4000".parse().unwrap()).unwrap());
                let mut client = KvsClient::new("127.0.0.1:4000".parse().unwrap());
                for i in 0..s.key.len() {
                    client
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
                // handle.join().unwrap();
            },
            BatchSize::SmallInput,
        );
    });
    let para = Para::new("kvs".to_string(), "RayonThreadPool".to_string(), 100);
    c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvStore::open(temp_dir.into_path()).unwrap();
                let cpus = num_cpus::get();
                let pool = RayonThreadPool::new(cpus as u32).unwrap();
                let mut server =
                    KvsServer::new(store, pool, slog::Logger::root(slog::Discard, o!())).unwrap();
                thread::spawn(move || server.run("127.0.0.1:4000".parse().unwrap()).unwrap());
                KvsClient::new("127.0.0.1:4000".parse().unwrap())
            },
            |mut client| {
                for i in 0..s.key.len() {
                    client
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    let para = Para::new("sled".to_string(), "RayonThreadPool".to_string(), 100);
    c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvSled::open(temp_dir.into_path()).unwrap();
                let cpus = num_cpus::get();
                let pool = RayonThreadPool::new(cpus as u32).unwrap();
                let mut server =
                    KvsServer::new(store, pool, slog::Logger::root(slog::Discard, o!())).unwrap();
                thread::spawn(move || server.run("127.0.0.1:4000".parse().unwrap()).unwrap());
                KvsClient::new("127.0.0.1:4000".parse().unwrap())
            },
            |mut client| {
                for i in 0..s.key.len() {
                    client
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn get_bench(c: &mut Criterion) {
    let para = Para::new("kvs".to_string(), "SharedQueueThreadPool".to_string(), 100);
    c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvStore::open(temp_dir.into_path()).unwrap();
                let cpus = num_cpus::get();
                let pool = SharedQueueThreadPool::new(cpus as u32).unwrap();
                let mut server =
                    KvsServer::new(store, pool, slog::Logger::root(slog::Discard, o!())).unwrap();
                thread::spawn(move || server.run("127.0.0.1:4000".parse().unwrap()).unwrap());
                let mut client = KvsClient::new("127.0.0.1:4000".parse().unwrap());
                for i in 0..s.key.len() {
                    client
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
                KvsClient::new("127.0.0.1:4000".parse().unwrap())
            },
            |mut client| {
                for i in 0..s.key.len() {
                    client.get(s.key[i].to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    let para = Para::new("kvs".to_string(), "RayonThreadPool".to_string(), 100);
    c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvStore::open(temp_dir.into_path()).unwrap();
                let cpus = num_cpus::get();
                let pool = RayonThreadPool::new(cpus as u32).unwrap();
                let mut server =
                    KvsServer::new(store, pool, slog::Logger::root(slog::Discard, o!())).unwrap();
                thread::spawn(move || server.run("127.0.0.1:4000".parse().unwrap()).unwrap());
                let mut client = KvsClient::new("127.0.0.1:4000".parse().unwrap());
                for i in 0..s.key.len() {
                    client
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
                KvsClient::new("127.0.0.1:4000".parse().unwrap())
            },
            |mut client| {
                for i in 0..s.key.len() {
                    client.get(s.key[i].to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    let para = Para::new("sled".to_string(), "RayonThreadPool".to_string(), 100);
    c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvSled::open(temp_dir.into_path()).unwrap();
                let cpus = num_cpus::get();
                let pool = RayonThreadPool::new(cpus as u32).unwrap();
                let mut server =
                    KvsServer::new(store, pool, slog::Logger::root(slog::Discard, o!())).unwrap();
                thread::spawn(move || server.run("127.0.0.1:4000".parse().unwrap()).unwrap());
                let mut client = KvsClient::new("127.0.0.1:4000".parse().unwrap());
                for i in 0..s.key.len() {
                    client
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
                KvsClient::new("127.0.0.1:4000".parse().unwrap())
            },
            |mut client| {
                for i in 0..s.key.len() {
                    client.get(s.key[i].to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, write_bench, get_bench);
criterion_main!(benches);
