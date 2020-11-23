// use std::{fmt, process::exit};

// use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
// use kvs::{KvSled, KvStore, KvsEngine};
// use rand::distributions::Alphanumeric;
// use rand::{thread_rng, Rng};
// use tempfile::TempDir;

// #[derive(Debug)]
// struct Para {
//     engine: String,
//     key: String,
//     value: String,
// }

// impl Para {
//     fn new(engine: String) -> Para {
//         Para {
//             engine,
//             key: random_string_with_length(),
//             value: random_string_with_length(),
//         }
//     }
// }

// impl fmt::Display for Para {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "({}: {})", self.key, self.value)
//     }
// }

// const RANDOM_LENGTH: usize = 100000;

// fn random_string_with_length() -> String {
//     let rand_string: String = thread_rng()
//         .sample_iter(&Alphanumeric)
//         .take(RANDOM_LENGTH)
//         .collect();

//     // println!("{}", rand_string);
//     rand_string
// }

// // fn do_something(s: &Para, store: &mut Box<dyn KvsEngine>) {
// //     store.set(s.key.to_owned(), s.value.to_owned());
// // }

// pub fn kvs_write(c: &mut Criterion) {
//     let para = Para::new("kvs".to_string());
//     let temp_dir = TempDir::new().unwrap();
//     let mut store: Box<dyn KvsEngine> = match para.engine.as_ref() {
//         "kvs" => Box::new(KvStore::open(temp_dir.path()).unwrap()),
//         "sled" => Box::new(KvSled::open(temp_dir.path()).unwrap()),
//         _ => exit(1),
//     };
//     c.bench_with_input(BenchmarkId::new("kvs_write", &para), &para, |b, s| {
//         b.iter(|| store.set(s.key.to_owned(), s.value.to_owned()));
//     });
// }

// criterion_group!(benches, kvs_write);
// criterion_main!(benches);

#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use kvs::{KvSled, KvStore, KvsEngine};
use rand::prelude::*;
// use sled::Db;
use std::iter;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    KvStore::open(temp_dir.into_path()).unwrap()
                },
                |mut store| {
                    for i in 1..(1 << 12) {
                        store.set(format!("key{}", i), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
    .with_function("sled", |b, _| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                KvSled::open(temp_dir.into_path()).unwrap()
            },
            |mut store| {
                for i in 1..(1 << 12) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    c.bench("set_bench", bench);
}

fn get_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            })
        },
        vec![8, 12, 16],
    )
    .with_function("sled", |b, i| {
        let temp_dir = TempDir::new().unwrap();
        let mut db = KvSled::open(temp_dir.path()).unwrap();
        for key_i in 1..(1 << i) {
            db.set(format!("key{}", key_i), "value".to_string())
                .unwrap();
        }
        let mut rng = SmallRng::from_seed([0; 16]);
        b.iter(|| {
            db.get(format!("key{}", rng.gen_range(1, 1 << i))).unwrap();
        })
    });
    c.bench("get_bench", bench);
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
