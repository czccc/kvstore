# Key/Value Storage System

This project is followed with [Talent Plan](https://github.com/pingcap/talent-plan) from [PingCAP University](https://university.pingcap.com/). The origin documents related to this project is copied into [doc](./doc) directory

## Feature

- Writen with pure Rust
- Support command:
    - **Put**: add a Key/Value pairs into storage 
    - **Get**: get the value with given key
    - **Remove**: remove Key/Value pairs from storage
- Backend storage engine:
    - `KvStore`: based on log structured storage
    - `KvSled`: based on extern crate [`sled`](https://github.com/spacejam/sled)
- Backend thread pool:
    - `NaiveThreadPool`: a simple wrapper of `thread::spawn`
    - `SharedQueueThreadPool`: use shared channel to receive job and execute within specified size of threads
    - `RayonThreadPool`: based on extern crate [`rayon`](https://docs.rs/rayon/1.5.0/rayon/)
- Support network query
    - `KvsServer`: a server listen on specified socket and process requests from client
    - `KvsClient`: a client connect to socket and send request to server
- Benchmark:
    - Engine benches of `KvStore` and `KvSled`
    - Thread pool benches of `NaiveThreadPool`, `SharedQueueThreadPool` and `RayonThreadPool`

## TODO

- [ ] add more comments which can parse with `cargo doc`
- [ ] finish Lock-free readers in lab-4
- [ ] add asynchrony feature using `tokio` or `futures`
- [ ] etc..