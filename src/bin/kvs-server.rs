#[macro_use]
extern crate log;

// use serde::{Deserialize, Serialize};
use std::{env::current_dir, fs, io::Write, net::SocketAddr};
use structopt::StructOpt;

const DEFAULT_ADDR: &str = "127.0.0.1:4000";
/// Default Engine tag file
const ENGINE_TAG_FILE: &str = ".engine";

use kvs::preclude::*;
use tonic::transport::Server;

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

    info!("Key Value Store Server");
    info!("  Version : {}", env!("CARGO_PKG_VERSION"));
    info!("  IP-PORT : {}", opt.addr);
    info!("  Engine  : {}", opt.engine);
    info!("  TdPool  : {}", opt.thread_pool);

    let addr = opt.addr;

    let server = KvsServerBuilder::new()
        .engine(opt.engine)
        .path(current_dir().unwrap())
        .thread_pool(opt.thread_pool)
        .num_threads(num_cpus::get())
        .build()?;

    // let server = KvRpcServer::new(server);
    // server.run(opt.addr)?;
    Server::builder()
        .add_service(KvRpcServer::new(server))
        .serve(addr)
        .await
        .map_err(|e| KvError::StringError(e.to_string()))?;

    Ok(())
}
