#[macro_use]
extern crate log;

// use serde::{Deserialize, Serialize};
use std::{
    env::{self, current_dir},
    fs,
    io::Write,
    net::SocketAddr,
};
use structopt::StructOpt;

const DEFAULT_ADDR: &str = "127.0.0.1:4000";
/// Default Engine tag file
const ENGINE_TAG_FILE: &str = ".engine";

use kvs::preclude::*;

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

    let root_path = current_dir().unwrap().join("tmp");
    fs::create_dir(root_path.clone()).unwrap_or(());
    env::set_current_dir(root_path.clone()).unwrap();

    let opt: Opt = Opt::from_args();
    write_engine_to_dir(&opt.engine)?;

    info!("Key Value Store Raft Server");
    info!("  Version : {}", env!("CARGO_PKG_VERSION"));

    let servers = KvsServer::builder()
        .set_engine(opt.engine.clone())
        .add_node("127.0.0.1:5001".parse().unwrap(), root_path.join("1"))
        .add_node("127.0.0.1:5002".parse().unwrap(), root_path.join("2"))
        .add_node("127.0.0.1:5003".parse().unwrap(), root_path.join("3"))
        .build();
    servers.start()
}
