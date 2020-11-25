use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::{KvSled, KvStore, KvsEngine};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::Rng;
use std::{fmt, process::exit};
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
    let engine_list = vec!["kvs", "sled"];
    for engine in engine_list {
        let para = Para::new(engine.to_string(), 100);
        c.bench_with_input(BenchmarkId::new("write_bench", &para), &para, |b, s| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let store: Box<dyn KvsEngine> = match para.engine.as_ref() {
                        "kvs" => Box::new(KvStore::open(temp_dir.into_path()).unwrap()),
                        "sled" => Box::new(KvSled::open(temp_dir.into_path()).unwrap()),
                        _ => exit(1),
                    };
                    store
                },
                |mut store| {
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
}

pub fn get_bench(c: &mut Criterion) {
    let engine_list = vec!["kvs", "sled"];
    for engine in engine_list {
        let para = Para::new(engine.to_string(), 1000);
        c.bench_with_input(BenchmarkId::new("get_bench", &para), &para, |b, s| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let mut store: Box<dyn KvsEngine> = match para.engine.as_ref() {
                        "kvs" => Box::new(KvStore::open(temp_dir.into_path()).unwrap()),
                        "sled" => Box::new(KvSled::open(temp_dir.into_path()).unwrap()),
                        _ => exit(1),
                    };
                    for i in 0..s.key.len() {
                        store
                            .set(s.key[i].to_owned(), s.value[i].to_owned())
                            .unwrap();
                    }
                    store
                },
                |mut store| {
                    for i in 0..s.key.len() {
                        store.get(s.key[i].to_owned()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
}
criterion_group!(benches, write_bench, get_bench);
criterion_main!(benches);
