#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use kvs::*;
// use serde::{Deserialize, Serialize};
use slog::Drain;
use std::{env::current_dir, fs, net::SocketAddr, str::FromStr};
use structopt::StructOpt;

const DEFAULT_ADDR: &str = "127.0.0.1:4000";

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
        default_value = "Auto",
        parse(try_from_str = parse_str_to_engine)
    )]
    engine: Engine,
}

fn parse_str_to_engine(src: &str) -> Result<Engine> {
    let previous = fs::read_to_string(ENGINE_TAG_FILE);
    if src == "Auto" {
        Ok(Engine::from_str(&previous.unwrap_or("kvs".to_string()))?)
    } else if previous.is_err() || src == previous.unwrap() {
        Ok(Engine::from_str(src)?)
    } else {
        Err(KvError::ParserError(src.to_string()))
    }
}

fn main() -> Result<()> {
    // let decorator = slog_term::TermDecorator::new().build();
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    let opt: Opt = Opt::from_args();

    let mut server = KvsServer::new(opt.engine, current_dir().unwrap(), opt.addr, logger)?;
    server.run()?;

    Ok(())
}
