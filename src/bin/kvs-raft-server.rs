#[macro_use]
extern crate log;

// use serde::{Deserialize, Serialize};
use std::{
    env::{self, current_dir},
    fs,
    io::Write,
    net::SocketAddr,
    sync::Arc,
};
use structopt::StructOpt;

const DEFAULT_ADDR: &str = "127.0.0.1:4000";
/// Default Engine tag file
const ENGINE_TAG_FILE: &str = ".engine";

use kvs::preclude::*;
use tokio::sync::mpsc::unbounded_channel;
use tonic::transport::{Channel, Server};

#[derive(Debug, StructOpt)]
#[structopt(
    name = env!("CARGO_PKG_NAME"), 
    about = env!("CARGO_PKG_DESCRIPTION"), 
    version = env!("CARGO_PKG_VERSION"), 
    author = env!("CARGO_PKG_AUTHORS")
)]
struct Opt {
    #[structopt(
        name = "IP-PORT",
        short = "a",
        long = "addr",
        default_value = DEFAULT_ADDR
    )]
    addr: SocketAddr,
    #[structopt(
        name = "ENGINE-NAME",
        short = "e",
        long = "engine",
        default_value = "auto",
        parse(try_from_str = parse_str_to_engine)
    )]
    engine: String,
    #[structopt(
        name = "THREADPOOL-NAME",
        short = "t",
        long = "threadpool",
        default_value = "rayon",
        parse(try_from_str = parse_str_to_pool)
    )]
    thread_pool: String,
}

fn parse_str_to_engine(src: &str) -> Result<String> {
    let previous = fs::read_to_string(ENGINE_TAG_FILE);
    if src == "auto" {
        Ok(previous.unwrap_or("kvs".to_string()))
    } else if previous.is_err() || src == previous.unwrap() {
        Ok(src.to_string())
    } else {
        Err(KvError::ParserError(src.to_string()))
    }
}

fn parse_str_to_pool(src: &str) -> Result<String> {
    if src == "rayon" {
        Ok(src.to_string())
    } else if src == "share" {
        Ok(src.to_string())
    } else if src == "naive" {
        Ok(src.to_string())
    } else {
        Err(KvError::ParserError(src.to_string()))
    }
}

fn write_engine_to_dir(engine: &String) -> Result<()> {
    let mut engine_tag_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(ENGINE_TAG_FILE)?;
    engine_tag_file.write(engine.as_bytes())?;
    engine_tag_file.flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let opt: Opt = Opt::from_args();
    write_engine_to_dir(&opt.engine)?;

    info!("Key Value Store Raft Server");
    info!("  Version : {}", env!("CARGO_PKG_VERSION"));

    let root_path = current_dir().unwrap().join("tmp");
    fs::create_dir(root_path.clone()).unwrap_or(());

    let addrs = vec![
        (1_usize, "http://127.0.0.1:5001"),
        (2, "http://127.0.0.1:5002"),
        (3, "http://127.0.0.1:5003"),
    ];
    let peers: Vec<RaftRpcClient<Channel>> = addrs
        .iter()
        .map(|(_me, addr)| Channel::from_static(*addr).connect_lazy().unwrap())
        .map(|res| RaftRpcClient::new(res))
        .collect();

    let addrs1 = vec![
        (0_usize, "127.0.0.1:5001"),
        (1, "127.0.0.1:5002"),
        (2, "127.0.0.1:5003"),
    ];
    let handles = addrs1
        .into_iter()
        .map(|(me, addr)| {
            let root_path = root_path.clone();
            let peers = peers.clone();
            let engine = opt.engine.clone();
            let threaded_rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            std::thread::spawn(move || {
                threaded_rt.block_on(async move {
                    let path = root_path.join(me.to_string());
                    fs::create_dir(path.clone()).unwrap_or(());
                    env::set_current_dir(path.clone()).unwrap();
                    let persister = Arc::new(FilePersister::new());
                    let (apply_ch_sender, apply_ch_receiver) = unbounded_channel();
                    let raft_node = RaftNode::new(peers, me, persister.clone(), apply_ch_sender);
                    let store = match engine.as_ref() {
                        "kvs" => EngineKind::kvs(KvStore::open(path.clone()).unwrap()),
                        "sled" => EngineKind::sled(KvSled::open(path.clone()).unwrap()),
                        unknown => Err(KvError::ParserError(unknown.to_string())).unwrap(),
                    };
                    let maxraftstate = Some(1024);
                    let kv_raft_node = KvRaftNode::new(
                        raft_node.clone(),
                        store,
                        me,
                        persister.clone(),
                        maxraftstate,
                        apply_ch_receiver,
                    );
                    Server::builder()
                        .add_service(RaftRpcServer::new(raft_node))
                        .add_service(KvRpcServer::new(kv_raft_node))
                        .serve(addr.parse().unwrap())
                        .await
                        .map_err(|e| KvError::StringError(e.to_string()))
                        .unwrap();
                });
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}
