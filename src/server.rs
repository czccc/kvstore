use crate::thread_pool::*;
use crate::*;
use std::{
    io::Write,
    io::{prelude::*, BufReader, BufWriter},
    net::SocketAddr,
    net::{TcpListener, TcpStream},
    ops::Bound::*,
    ops::RangeBounds,
    str::from_utf8,
};

/// Kvs Server
pub struct KvsServer<E: KvsBackend, P: ThreadPool> {
    store: E,
    thread_pool: P,
}

impl<E: KvsBackend, P: ThreadPool> KvsServer<E, P> {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: E, thread_pool: P) -> Result<Self> {
        Ok(KvsServer { store, thread_pool })
    }
    /// Run Kvs Server at given Addr
    pub fn run(&mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        info!("[Server] Listening on {}", addr);

        // accept connections and process them serially
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let store = self.store.clone();
                    self.thread_pool.spawn(move || {
                        handle_request(store, stream).unwrap();
                    })
                }
                Err(e) => println!("{}", e),
            }
        }
        Ok(())
    }
}

fn handle_request<E: KvsBackend>(store: E, stream: TcpStream) -> Result<()> {
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

    // info!(
    //     "[Server] Received request {}, Response: {}",
    //     request_str, response_str
    // );

    Ok(())
}

fn process_request<E: KvsBackend>(store: E, req: Request) -> Response {
    match req.cmd.as_str() {
        "Get" => {
            loop {
                let range = generate_range(&req.key, "lock", Some(0), Some(req.ts));
                let lock = store.range_last(range).unwrap();
                if let Some((k, v)) = lock {
                    info!("Get previous Lock in Key: {:?}, Lock: {:?}", k, v);
                    // self.back_off_maybe_clean_up_lock(req.ts, key.to_owned());
                    continue;
                }

                let range = generate_range(&req.key, "write", Some(0), Some(req.ts));
                let last_write = store.range_last(range).unwrap();
                if last_write.is_none() {
                    return Response {
                        status: "ok".to_string(),
                        result: Some("Key not found".to_string()),
                    };
                }
                if let Ok(data_ts) = last_write.unwrap().1.parse() {
                    let range = generate_range(&req.key, "data", Some(data_ts), Some(data_ts));
                    let (_, value) = store.range_last(range).unwrap().unwrap();
                    return Response {
                        status: "ok".to_string(),
                        result: {
                            if value.is_empty() {
                                Some("Key not found".to_string())
                            } else {
                                Some(value)
                            }
                        },
                    };
                } else {
                    return Response {
                        status: "err".to_string(),
                        result: Some("Parser Write int Error".to_string()),
                    };
                }
            }
        }
        "TSO" => match store.next_timestamp() {
            Ok(ts) => Response {
                status: "ok".to_string(),
                result: Some(ts.to_string()),
            },
            Err(_) => Response {
                status: "err".to_string(),
                result: Some("Error in get TimeStamp".to_string()),
            },
        },
        "PreWrite" => {
            // let is_set = req.value.is_some();
            let key = req.key.as_ref();
            let read_range = generate_range(key, "write", Some(req.ts), None);
            match store.range_last(read_range) {
                Ok(Some(_)) => {
                    return Response {
                        status: "err".to_string(),
                        result: Some("Abort on writes after our start timestamp".to_string()),
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    return Response {
                        status: "err".to_string(),
                        result: Some(e.to_string()),
                    }
                }
            }
            let read_range = generate_range(key, "lock", None, None);
            match store.range_last(read_range) {
                Ok(Some(_)) => {
                    return Response {
                        status: "err".to_string(),
                        result: Some("Abort on lock at any timestamp".to_string()),
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    return Response {
                        status: "err".to_string(),
                        result: Some(e.to_string()),
                    }
                }
            }

            let write_key = generate_key(key, "data", req.ts);
            match store.set(write_key, req.value.unwrap()) {
                Ok(_) => {}
                Err(_) => {
                    return Response {
                        status: "err".to_string(),
                        result: Some("Set Error!".to_string()),
                    }
                }
            };
            let write_key = generate_key(key, "lock", req.ts);
            match store.set(write_key, req.primary) {
                Ok(_) => {}
                Err(_) => {
                    return Response {
                        status: "err".to_string(),
                        result: Some("Set Error!".to_string()),
                    }
                }
            };
            Response {
                status: "ok".to_owned(),
                result: None,
            }
        }
        "Commit" => {
            if req.primary == req.key {
                let read_range = generate_range(&req.key, "lock", Some(req.ts), Some(req.ts));
                match store.range_last(read_range) {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return Response {
                            status: "err".to_string(),
                            result: Some("Abort on lock clear resolved by others".to_string()),
                        }
                    }
                    Err(e) => {
                        return Response {
                            status: "err".to_string(),
                            result: Some(e.to_string()),
                        }
                    }
                }

                let write_key = generate_key(&req.key, "write", req.commit_ts);
                match store.set(write_key, req.ts.to_string()) {
                    Ok(_) => {}
                    Err(_) => {
                        return Response {
                            status: "err".to_string(),
                            result: Some("Set Error!".to_string()),
                        }
                    }
                };
                let remove_key = generate_key(&req.key, "lock", req.ts);
                match store.remove(remove_key) {
                    Ok(_) => {}
                    Err(_) => {
                        return Response {
                            status: "err".to_string(),
                            result: Some("Remove Error!".to_string()),
                        }
                    }
                };
                Response {
                    status: "ok".to_owned(),
                    result: None,
                }
            } else {
                let write_key = generate_key(&req.key, "write", req.commit_ts);
                match store.set(write_key, req.ts.to_string()) {
                    Ok(_) => {}
                    Err(_) => {
                        return Response {
                            status: "err".to_string(),
                            result: Some("Set Error!".to_string()),
                        }
                    }
                };
                let remove_key = generate_key(&req.key, "lock", req.ts);
                match store.remove(remove_key) {
                    Ok(_) => {}
                    Err(_) => {
                        return Response {
                            status: "err".to_string(),
                            result: Some("Remove Error!".to_string()),
                        }
                    }
                };
                Response {
                    status: "ok".to_owned(),
                    result: None,
                }
            }
        }
        _ => Response {
            status: "err".to_string(),
            result: Some("Unknown Command!".to_string()),
        },
    }
}

fn generate_key(key: &str, col: &str, ts: u64) -> String {
    format!("{}-{}-{:020}", key, col, ts)
}

fn generate_range(
    key: &str,
    col: &str,
    start: Option<u64>,
    end: Option<u64>,
) -> impl RangeBounds<String> {
    // key + "-" + col + &ts.to_string()

    let key_start_inclusive = start.map_or(Included(generate_key(key, col, u64::MIN)), |v| {
        Included(generate_key(key, col, v))
    });
    let key_end_inclusive = end.map_or(Included(generate_key(key, col, u64::MAX)), |v| {
        Included(generate_key(key, col, v))
    });
    (key_start_inclusive, key_end_inclusive)
}
