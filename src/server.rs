use crate::thread_pool::*;
use crate::*;
use std::{
    collections::HashMap,
    io::Write,
    io::{prelude::*, BufReader, BufWriter},
    net::SocketAddr,
    net::{TcpListener, TcpStream},
    ops::Bound::*,
    ops::RangeBounds,
    str::from_utf8,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Kvs Server
pub struct KvsServer<E: KvsBackend, P: ThreadPool> {
    store: E,
    thread_pool: P,
    pending_lock: Arc<Mutex<HashMap<String, Instant>>>,
}

impl<E: KvsBackend, P: ThreadPool> KvsServer<E, P> {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: E, thread_pool: P) -> Result<Self> {
        Ok(KvsServer {
            store,
            thread_pool,
            pending_lock: Arc::new(Mutex::new(HashMap::new())),
        })
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
                    let pending_lock = self.pending_lock.clone();
                    self.thread_pool.spawn(move || {
                        handle_request(store, stream, pending_lock).unwrap();
                    })
                }
                Err(e) => println!("{}", e),
            }
        }
        Ok(())
    }
}

fn handle_request<E: KvsBackend>(
    store: E,
    stream: TcpStream,
    pending_lock: Arc<Mutex<HashMap<String, Instant>>>,
) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let mut buf = vec![];
    let _len = reader.read_until(b'}', &mut buf)?;
    let request_str = from_utf8(&buf).unwrap();

    let request: Request = serde_json::from_str(request_str)?;
    let response = process_request(store, request, pending_lock);

    let response_str = serde_json::to_string(&response)?;
    writer.write(&response_str.as_bytes())?;
    writer.flush()?;

    info!(
        "[Server] Received request {}, Response: {}",
        request_str, response_str
    );

    Ok(())
}

fn process_request<E: KvsBackend>(
    store: E,
    req: Request,
    pending_lock: Arc<Mutex<HashMap<String, Instant>>>,
) -> Response {
    match req.cmd.as_str() {
        "Get" => {
            loop {
                let range = generate_range(&req.key, "lock", Some(0), Some(req.ts));
                let lock = store.range_last(range).unwrap();
                if let Some((_, v)) = lock {
                    // Found previous lock. Wait for back_off_maybe_clean_up_lock
                    // info!("Get previous Lock in Key: {:?}, Lock: {:?}", k, v);
                    let primary_ts: u64 = v.split_terminator("-").last().unwrap().parse().unwrap();
                    let primary_key = v.split_terminator("-").take(1).next().unwrap();
                    back_off_maybe_clean_up_lock(
                        store.clone(),
                        req.key.to_owned(),
                        primary_key.to_owned(),
                        primary_ts,
                        pending_lock.clone(),
                        req.ts,
                    );
                    continue;
                }

                let range = generate_range(&req.key, "write", Some(0), Some(req.ts));
                let last_write = store.range_last(range).unwrap();
                if last_write.is_none() {
                    // Not found previous write
                    return Response {
                        status: "ok".to_string(),
                        result: Some("Key not found".to_string()),
                    };
                }
                if let Ok(data_ts) = last_write.unwrap().1.parse() {
                    // Found previous write in data_ts, read it and return.
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
            let key = req.key.as_ref();
            let read_range = generate_range(key, "write", Some(req.ts), None);
            match store.range_last(read_range) {
                Ok(Some(_)) => {
                    return Response {
                        status: "err".to_string(),
                        result: Some("Abort on writes after txn start timestamp".to_string()),
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
            let primary_key = generate_key(&req.primary, "lock", req.ts);
            match store.set(write_key, primary_key) {
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
                let primary_key = generate_key(&req.primary, "lock", req.ts);
                if pending_lock.lock().unwrap().contains_key(&primary_key) {
                    pending_lock
                        .lock()
                        .unwrap()
                        .insert(primary_key, Instant::now());
                }
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
                let primary_key = generate_key(&req.primary, "lock", req.ts);
                if pending_lock.lock().unwrap().contains_key(&primary_key) {
                    pending_lock
                        .lock()
                        .unwrap()
                        .insert(primary_key, Instant::now());
                }
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
    let key_start_inclusive = start.map_or(Included(generate_key(key, col, u64::MIN)), |v| {
        Included(generate_key(key, col, v))
    });
    let key_end_inclusive = end.map_or(Included(generate_key(key, col, u64::MAX)), |v| {
        Included(generate_key(key, col, v))
    });
    (key_start_inclusive, key_end_inclusive)
}

fn back_off_maybe_clean_up_lock<E: KvsBackend>(
    store: E,
    key: String,
    primary_key: String,
    primary_ts: u64,
    pending_lock: Arc<Mutex<HashMap<String, Instant>>>,
    ts: u64,
) {
    const TTL: u64 = Duration::from_millis(1000).as_nanos() as u64;
    let mut pending_lock = pending_lock.lock().unwrap();
    let primary_lock_key = generate_key(&primary_key, "lock", primary_ts);
    if !pending_lock.contains_key(&primary_lock_key) {
        // No primary lock in pending, add it and wait TTL
        info!("Pending Add Key: {:?}", primary_lock_key);
        pending_lock.insert(primary_lock_key, Instant::now());
    } else {
        let last_instant = pending_lock
            .get(&primary_lock_key)
            .map(|v| v.to_owned())
            .unwrap();
        if last_instant.elapsed().as_nanos() as u64 >= TTL {
            // TTL is elapsed. Check primary lock is cleaned up or not
            let range = generate_range(&primary_key, "lock", Some(0), Some(ts));
            let lock = store.range_last(range).unwrap();
            if lock.is_some() {
                // primary lock haven't been cleaned.
                // But TTL is elapsed. So clean this primary lock
                info!("Pending Remove Key: {:?} and Roll back", primary_lock_key);
                pending_lock.remove(&primary_lock_key);
                let _ = store.remove(primary_lock_key);
            } else {
                // primary lock have been cleaned.
                // Check it is committed or not in write column
                let write_range = generate_range(&primary_key, "write", Some(primary_ts), None);
                if let Some((primary_commit_ts, _)) =
                    store.range_last(write_range).unwrap().map(|(k, v)| {
                        (
                            k.split_terminator("-")
                                .last()
                                .unwrap()
                                .parse::<u64>()
                                .unwrap(),
                            v,
                        )
                    })
                {
                    // Find write. So Roll Forward to commit this key and remove its lock
                    info!(
                        "Pending Remove Key: {:?} and Roll forward",
                        primary_lock_key
                    );
                    pending_lock.remove(&primary_lock_key);
                    let _ = store.set(
                        generate_key(&key, "write", primary_commit_ts),
                        primary_ts.to_string(),
                    );
                    let _ = store.remove(generate_key(&key, "lock", primary_ts));
                } else {
                    // No write. So clean this lock
                    info!("Pending Remove Key: {:?} and Roll back", key);
                    let _ = store.remove(generate_key(&key, "lock", primary_ts));
                    pending_lock.remove(&primary_lock_key);
                }
            }
        }
    }
}
