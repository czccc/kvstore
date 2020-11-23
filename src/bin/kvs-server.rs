#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use kvs::{KvError, KvSled, KvStore, KvsEngine, Request, Response, Result};
// use serde::{Deserialize, Serialize};
use std::{
    env::current_dir,
    fs,
    io::{prelude::*, BufReader, BufWriter},
    net::SocketAddr,
    net::{TcpListener, TcpStream},
    str::from_utf8,
};
use std::{io::Write, str::FromStr};
use structopt::StructOpt;

use slog::Drain;

const ENGINE_TAG: &str = ".engine";

#[allow(non_camel_case_types)]
#[derive(Debug)]
enum Engine {
    kvs,
    sled,
}

impl FromStr for Engine {
    type Err = KvError;

    fn from_str(s: &str) -> Result<Self> {
        if s == "kvs" {
            Ok(Engine::kvs)
        } else if s == "sled" {
            Ok(Engine::sled)
        } else {
            Err(KvError::ParserError(s.to_string()))
        }
    }
}

fn parse_str(src: &str) -> Result<Engine> {
    let previous = fs::read_to_string(ENGINE_TAG);
    // match pre {}
    if src == "Auto" {
        Ok(Engine::from_str(match &previous {
            Ok(x) => x,
            Err(_) => "kvs",
        })?)
    } else if previous.is_err() || src == previous.unwrap() {
        Ok(Engine::from_str(src)?)
    } else {
        Err(KvError::ParserError(src.to_string()))
    }
}

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
        default_value = "127.0.0.1:4000"
    )]
    addr: SocketAddr,
    #[structopt(
        name = "ENGINE-NAME",
        short = "e",
        long = "engine",
        default_value = "Auto",
        parse(try_from_str = parse_str)
    )]
    engine: Engine,
}

// fn check_valid_engine(engine: &Engine) -> Result<()> {
//     fs::read_to_string(ENGINE_TAG).unwrap_or("kvs".to_string())
// }

fn write_engine_to_dir(engine: &Engine) -> Result<()> {
    let mut engine_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(ENGINE_TAG)?;
    match engine {
        Engine::kvs => engine_file.write(b"kvs")?,
        Engine::sled => engine_file.write(b"sled")?,
    };
    engine_file.flush()?;
    Ok(())
}

fn handle_server(store: &mut Box<dyn KvsEngine>, stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    // let mut buf = [0 as u8; 10];
    let mut buf = vec![];
    let len = reader.read_until(b'}', &mut buf)?;
    println!("{}: {}", len, from_utf8(&buf).unwrap());
    let request: Request = serde_json::from_str(from_utf8(&buf).unwrap())?;

    let response = process_request(store, request);

    let response = serde_json::to_string(&response)?;
    writer.write(&response.as_bytes())?;
    writer.flush()?;

    Ok(())
}

fn process_request(store: &mut Box<dyn KvsEngine>, req: Request) -> Response {
    match req.cmd.as_str() {
        "Get" => match store.get(req.key) {
            Ok(Some(value)) => Response {
                status: "ok".to_string(),
                message: None,
                result: Some(value),
            },
            Ok(None) => Response {
                status: "err".to_string(),
                message: Some("Key not found".to_string()),
                result: None,
            },
            Err(_) => Response {
                status: "err".to_string(),
                message: Some("Get Error!".to_string()),
                result: None,
            },
        },
        "Set" => match store.set(req.key, req.value.unwrap()) {
            Ok(_) => Response {
                status: "ok".to_string(),
                message: None,
                result: None,
            },
            Err(_) => Response {
                status: "err".to_string(),
                message: Some("Set Error!".to_string()),
                result: None,
            },
        },
        "Remove" => match store.remove(req.key) {
            Ok(_) => Response {
                status: "ok".to_string(),
                message: None,
                result: None,
            },
            Err(_) => Response {
                status: "err".to_string(),
                message: Some("Key not found".to_string()),
                result: None,
            },
        },
        _ => Response {
            status: "err".to_string(),
            message: Some("Unknown Error!".to_string()),
            result: None,
        },
    }
}

fn main() -> Result<()> {
    // let decorator = slog_term::TermDecorator::new().build();
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let _log = slog::Logger::root(drain, o!());

    let opt: Opt = Opt::from_args();
    write_engine_to_dir(&opt.engine)?;
    let server = _log.new(o!());
    info!(server, "Version : {}", env!("CARGO_PKG_VERSION"));
    info!(server, "IP-PORT : {}", opt.addr);
    info!(server, "Engine  : {:?}", opt.engine);
    println!("{:?}", opt);

    let mut store: Box<dyn KvsEngine> = match opt.engine {
        Engine::kvs => Box::new(KvStore::open(current_dir().unwrap())?),
        Engine::sled => Box::new(KvSled::open(current_dir().unwrap())?),
    };

    let listener = TcpListener::bind(opt.addr)?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // println!("new client!");
                handle_server(&mut store, stream)?;
            }
            Err(e) => println!("{}", e),
        }
    }

    Ok(())
}
