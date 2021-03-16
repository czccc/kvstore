#[macro_use]
extern crate log;

// use serde::{Deserialize, Serialize};
use std::{env::current_dir, fs, io::Write, net::SocketAddr};
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
        name = "Server Kind",
        short = "s",
        long = "server",
        default_value = "basic"
    )]
    server: String,
    #[structopt(
        name = "Engine Name",
        short = "e",
        long = "engine",
        default_value = "auto",
        parse(try_from_str = parse_str_to_engine)
    )]
    engine: String,
    #[structopt(
        name = "IP-PORT",
        short = "a",
        long = "addr",
        default_value = DEFAULT_ADDR
    )]
    addrs: Vec<SocketAddr>,
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
    info!("  IP-PORT : {:?}", opt.addrs);
    info!("  Engine  : {}", opt.engine);

    let server = KvsServer::builder()
        .set_server(opt.server)
        .set_engine(opt.engine)
        .set_root_path(current_dir().unwrap())
        .add_batch_nodes(opt.addrs);

    let server = server.build();
    server.start()
}
