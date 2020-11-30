use slog::Logger;

use crate::thread_pool::*;
use crate::*;
use std::{
    io::Write,
    io::{prelude::*, BufReader, BufWriter},
    net::SocketAddr,
    net::{TcpListener, TcpStream},
    str::from_utf8,
};

/// Kvs Server
pub struct KvsServer<E: KvsEngine> {
    store: E,
    addr: SocketAddr,
    logger: slog::Logger,
}

impl<E: KvsEngine> KvsServer<E> {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: E, addr: SocketAddr, logger: slog::Logger) -> Result<Self> {
        Ok(KvsServer {
            store,
            addr,
            logger,
        })
    }
    /// Run Kvs Server at given Addr
    pub fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.addr)?;

        info!(self.logger, "Listening on {}", self.addr);

        let thread_pool = thread_pool::NaiveThreadPool::new(10)?;

        // accept connections and process them serially
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let store = self.store.clone();
                    let logger = self.logger.clone();
                    thread_pool.spawn(move || {
                        handle_request(store, stream, logger).unwrap();
                    })
                }
                Err(e) => println!("{}", e),
            }
        }
        Ok(())
    }
}

fn handle_request<E: KvsEngine>(store: E, stream: TcpStream, logger: Logger) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let mut buf = vec![];
    let _len = reader.read_until(b'}', &mut buf)?;
    let request_str = from_utf8(&buf).unwrap();

    let request: Request = serde_json::from_str(request_str)?;
    let response = process_request(store, request);

    let response_str = serde_json::to_string(&response)?;
    writer.write(&response_str.as_bytes())?;
    writer.flush()?;

    info!(
        logger,
        "Received request from {} - Args: {}, Response: {}",
        stream.local_addr()?,
        request_str,
        response_str
    );

    Ok(())
}

fn process_request<E: KvsEngine>(store: E, req: Request) -> Response {
    match req.cmd.as_str() {
        "Get" => match store.get(req.key) {
            Ok(Some(value)) => Response {
                status: "ok".to_string(),
                result: Some(value),
            },
            Ok(None) => Response {
                status: "ok".to_string(),
                result: Some("Key not found".to_string()),
            },
            Err(_) => Response {
                status: "err".to_string(),
                result: Some("Something Wrong!".to_string()),
            },
        },
        "Set" => match store.set(req.key, req.value.unwrap()) {
            Ok(_) => Response {
                status: "ok".to_string(),
                result: None,
            },
            Err(_) => Response {
                status: "err".to_string(),
                result: Some("Set Error!".to_string()),
            },
        },
        "Remove" => match store.remove(req.key) {
            Ok(_) => Response {
                status: "ok".to_string(),
                result: None,
            },
            Err(_) => Response {
                status: "err".to_string(),
                result: Some("Key not found".to_string()),
            },
        },
        _ => Response {
            status: "err".to_string(),
            result: Some("Unknown Command!".to_string()),
        },
    }
}
