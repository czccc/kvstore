use assert_cmd::prelude::CommandCargoExt;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use kvs::KvsClient;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::Rng;
use std::{
    fmt,
    process::Command,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
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

fn thread_pool_write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("thread_pool_write");
    vec!["kvs", "sled"]
        .iter()
        .map(|engine| {
            vec!["naive", "share", "rayon"]
                .iter()
                .map(|pool| {
                    let para = Para::new(engine.to_string(), pool.to_string(), 100);
                    group.bench_with_input(
                        BenchmarkId::new(engine.to_string() + "-" + pool, &para),
                        &para,
                        |b, s| {
                            b.iter_custom(|_iters| {
                                let (sender, receiver) = mpsc::sync_channel(0);
                                let temp_dir = TempDir::new().unwrap();
                                let mut server = Command::cargo_bin("kvs-server").unwrap();
                                let mut child = server
                                    .args(&[
                                        "--engine",
                                        &para.engine,
                                        "--threadpool",
                                        &para.pool,
                                        "--addr",
                                        "127.0.0.1:5000",
                                    ])
                                    .env("RUST_LOG", "warn")
                                    .current_dir(&temp_dir)
                                    .spawn()
                                    .unwrap();
                                let handle = thread::spawn(move || {
                                    let _ = receiver.recv(); // wait for main thread to finish
                                    child.kill().expect("server exited before killed");
                                });
                                thread::sleep(Duration::from_secs(1));

                                let start = Instant::now();
                                for i in 0..s.key.len() {
                                    let mut client =
                                        KvsClient::new("127.0.0.1:5000".parse().unwrap());
                                    client
                                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                                        .expect("Unable set");
                                }
                                let ret = start.elapsed();

                                sender.send(()).unwrap();
                                handle.join().unwrap();
                                ret
                            });
                        },
                    );
                })
                .for_each(|_| {})
        })
        .for_each(|_| {});
    group.finish();
}

fn thread_pool_get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("thread_pool_get");
    vec!["kvs", "sled"]
        .iter()
        .map(|engine| {
            vec!["naive", "share", "rayon"]
                .iter()
                .map(|pool| {
                    let para = Para::new(engine.to_string(), pool.to_string(), 100);
                    group.bench_with_input(
                        BenchmarkId::new(engine.to_string() + "-" + pool, &para),
                        &para,
                        |b, s| {
                            b.iter_custom(|_iters| {
                                let (sender, receiver) = mpsc::sync_channel(0);
                                let temp_dir = TempDir::new().unwrap();
                                let mut server = Command::cargo_bin("kvs-server").unwrap();
                                let mut child = server
                                    .args(&[
                                        "--engine",
                                        &para.engine,
                                        "--threadpool",
                                        &para.pool,
                                        "--addr",
                                        "127.0.0.1:5000",
                                    ])
                                    .env("RUST_LOG", "warn")
                                    .current_dir(&temp_dir)
                                    .spawn()
                                    .unwrap();
                                let handle = thread::spawn(move || {
                                    let _ = receiver.recv(); // wait for main thread to finish
                                    child.kill().expect("server exited before killed");
                                });
                                thread::sleep(Duration::from_secs(1));

                                for i in 0..s.key.len() {
                                    let mut client =
                                        KvsClient::new("127.0.0.1:5000".parse().unwrap());
                                    client
                                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                                        .expect("Unable set");
                                }

                                let start = Instant::now();
                                for i in 0..s.key.len() {
                                    let mut client =
                                        KvsClient::new("127.0.0.1:5000".parse().unwrap());
                                    client.get(s.key[i].to_owned()).expect("Unable get");
                                }
                                let ret = start.elapsed();

                                sender.send(()).unwrap();
                                handle.join().unwrap();
                                ret
                            });
                        },
                    );
                })
                .for_each(|_| {})
        })
        .for_each(|_| {});
    group.finish();
}

criterion_group!(benches, thread_pool_write_bench, thread_pool_get_bench);
criterion_main!(benches);
