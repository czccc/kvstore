use crate::*;
use std::{
    fs,
    io::Write,
    io::{prelude::*, BufReader, BufWriter},
    net::SocketAddr,
    net::{TcpListener, TcpStream},
    path::PathBuf,
    str::from_utf8,
};

/// Kvs Server
pub struct KvsServer {
    store: Box<dyn KvsEngine>,
    addr: SocketAddr,
    logger: slog::Logger,
}

impl KvsServer {
    /// Construct a new Kvs Server
    pub fn new(
        engine: Engine,
        path: impl Into<PathBuf>,
        addr: SocketAddr,
        logger: slog::Logger,
    ) -> Result<Self> {
        let store = engine.open(path)?;
        write_engine_to_dir(&engine)?;

        info!(logger, "Key Value Store Server");
        info!(logger, "Version : {}", env!("CARGO_PKG_VERSION"));
        info!(logger, "IP-PORT : {}", addr);
        info!(logger, "Engine  : {:?}", engine);
        // info!(logger, "FileDir : {:?}", path.into());

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

        // accept connections and process them serially
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    self.handle_request(stream)?;
                }
                Err(e) => println!("{}", e),
            }
        }
        Ok(())
    }

    fn handle_request(&mut self, stream: TcpStream) -> Result<()> {
        let mut reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);

        let mut buf = vec![];
        let _len = reader.read_until(b'}', &mut buf)?;
        let request_str = from_utf8(&buf).unwrap();

        let request: Request = serde_json::from_str(request_str)?;
        let response = self.process_request(request);

        let response_str = serde_json::to_string(&response)?;
        writer.write(&response_str.as_bytes())?;
        writer.flush()?;

        info!(
            self.logger,
            "Received request from {} - Args: {}, Response: {}",
            stream.local_addr()?,
            request_str,
            response_str
        );

        Ok(())
    }

    fn process_request(&mut self, req: Request) -> Response {
        match req.cmd.as_str() {
            "Get" => match self.store.get(req.key) {
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
            "Set" => match self.store.set(req.key, req.value.unwrap()) {
                Ok(_) => Response {
                    status: "ok".to_string(),
                    result: None,
                },
                Err(_) => Response {
                    status: "err".to_string(),
                    result: Some("Set Error!".to_string()),
                },
            },
            "Remove" => match self.store.remove(req.key) {
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
}

fn write_engine_to_dir(engine: &Engine) -> Result<()> {
    let mut engine_tag_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(ENGINE_TAG_FILE)?;
    match engine {
        Engine::kvs => engine_tag_file.write(b"kvs")?,
        Engine::sled => engine_tag_file.write(b"sled")?,
    };
    engine_tag_file.flush()?;
    Ok(())
}
