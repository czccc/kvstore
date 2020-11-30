use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::{KvSled, KvStore, KvsEngine};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::Rng;
use std::fmt;
use tempfile::TempDir;

#[derive(Debug)]
struct Para {
    engine: String,
    key: Vec<String>,
    value: Vec<String>,
}

impl Para {
    fn new(engine: String, len: usize) -> Para {
        let mut rng: StdRng = rand::SeedableRng::seed_from_u64(1);
        Para {
            engine,
            key: random_string_with_length(&mut rng, len),
            value: random_string_with_length(&mut rng, len),
        }
    }
}

impl fmt::Display for Para {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(Engine: {}, Length: {})", self.engine, self.key.len())
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

pub fn write_bench(c: &mut Criterion) {
    let para = Para::new("kvs".to_string(), 100);
    c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvStore::open(temp_dir.into_path()).unwrap();
                store
            },
            |store| {
                for i in 0..s.key.len() {
                    store
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    let para = Para::new("sled".to_string(), 100);
    c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvSled::open(temp_dir.into_path()).unwrap();
                store
            },
            |store| {
                for i in 0..s.key.len() {
                    store
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

pub fn get_bench(c: &mut Criterion) {
    let para = Para::new("kvs".to_string(), 1000);
    c.bench_with_input(BenchmarkId::new("get_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvStore::open(temp_dir.into_path()).unwrap();
                for i in 0..s.key.len() {
                    store
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
                store
            },
            |store| {
                for i in 0..s.key.len() {
                    store.get(s.key[i].to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    let para = Para::new("sled".to_string(), 1000);
    c.bench_with_input(BenchmarkId::new("get_bench", &para), &para, |b, s| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let store = KvSled::open(temp_dir.into_path()).unwrap();
                for i in 0..s.key.len() {
                    store
                        .set(s.key[i].to_owned(), s.value[i].to_owned())
                        .unwrap();
                }
                store
            },
            |store| {
                for i in 0..s.key.len() {
                    store.get(s.key[i].to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, write_bench, get_bench);
criterion_main!(benches);
