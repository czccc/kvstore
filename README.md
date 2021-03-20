# Key/Value Storage System

This project is started with [Talent Plan](https://github.com/pingcap/talent-plan) from [PingCAP University](https://university.pingcap.com/). The origin documents related to this project is copied into [doc](./doc) directory. After that, this project add some more feature including `Raft` consensus, `Percolator` transaction, and RPC operations

## Feature

- Writen with Rust
- Support transaction command:
    - **Txn**: start a new transaction
    - **Set**: add a Key/Value pairs into storage 
    - **Get**: get the value with given key
    - **Remove**: remove Key/Value pairs from storage
- Backend storage engine:
    - `KvStore`: based on log structured storage
    - `KvSled`: based on extern crate [`sled`](https://github.com/spacejam/sled)
- Multiple server kinds:
    - `basic`: use a single server to handle requests, supports `Percolator` transaction
    - `raft`: use multiple raft nodes as a whole server, supports `Percolator` transaction also
- Support RPC: (based on `tonic` crate)
    - `KvRpc`: a RPC that interacts with client, supports transaction command
    - `RaftRpc`: a RPC which used between raft nodes, supports leader election, heartbeat, append entries, and install snapshot
- Backend thread pool (deprecated because use `tonic` RPC and `tokio` runtime):
    - `NaiveThreadPool`: a simple wrapper of `thread::spawn`
    - `SharedQueueThreadPool`: use shared channel to receive job and execute within specified size of threads
    - `RayonThreadPool`: based on extern crate [`rayon`](https://docs.rs/rayon/1.5.0/rayon/)
- Benchmark:
    - Engine benches of `KvStore` and `KvSled`
    - Thread pool benches of `NaiveThreadPool`, `SharedQueueThreadPool` and `RayonThreadPool`

## Example

### Server

For a server, you can use its `builder` to create a builder to specify some message:

- backend engine, which can be `kvs` or `sled`
- the server kind, which can be `basic` or `raft`
- server directory path, which can be set with `set_root_path`. It will make all server node files save in this root directory, for different server, its path will be `root_path/server-{i}` (`i` is its index). It is useful to create many server without specify all node's path. You can alse specify each server with a specific path with `add_node` function,
- listening address, which can be set with `add_batch_nodes` with a vector of addr or `add_node` with a single addr

After that, you can use `build` to get the server instance. `server.start()` will start this server by listening on specified address.

You can find [`src/bin/kvs-server.rs`](src/bin/kvs-server.rs) to see more details.

```rs
#[tokio::main]
async fn main() -> Result<()> {
    let addrs = vec!["127.0.0.1:4000", "127.0.0.1:4001", "127.0.0.1:4002"];
    let server = KvsServer::builder()
        .set_engine("kvs")
        .set_server("raft")
        .set_root_path(current_dir().unwrap())
        // .add_node("127.0.0.1:4000", current_dir().unwrap())
        .add_batch_nodes(addrs);
        .build();
        
    server.start()
}
```

### Client

For a client, it need to know all the server address which will be used. Likewise, you can use `add_batch_nodes` to add a batch of node addresses, or you can use `add_node` to add a node address. A little difference is client does not need to set the directory path.

After that you can start a transaction (`txn_start`), get some value of specific key (`txn_get`), set or delete some key value pairs (`txn_set`, `txn_delete`) and commit these change you made (`txn_commit`).

There are some other useful methods for getting (`get`), setting (`set`) and deleting (`remove`) one key, if you just want to make one command, you can use them for convenience.

You can find [`src/bin/kvs-client.rs`](src/bin/kvs-client.rs) to see more details.

```rs
#[tokio::main]
async fn main() -> Result<()> {
    let addrs = vec!["127.0.0.1:4000", "127.0.0.1:4001", "127.0.0.1:4002"];
    let mut client = KvsClient::builder()
        .add_batch_nodes(addrs)
        // .add_node("127.0.0.1:4000")
        .build();
    client.txn_start().await?;
    client.txn_get("key").await?;
    client.txn_set("key", "value")?;
    client.txn_commit().await?
}
```