use crate::*;
use crate::{backend::EngineKind, thread_pool::*};
// use std::{
//     io::Write,
//     io::{prelude::*, BufReader, BufWriter},
//     net::SocketAddr,
//     net::{TcpListener, TcpStream},
//     str::from_utf8,
// };
use crate::rpc::kvs_service::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Kvs Server
#[derive(Debug, Clone)]
pub struct KvsServer {
    store: Arc<EngineKind>,
    // thread_pool: ThreadPoolKind,
}

#[tonic::async_trait]
impl KvRpc for KvsServer {
    async fn set(
        &self,
        req: Request<SetRequest>,
    ) -> std::result::Result<Response<SetReply>, Status> {
        let req = req.into_inner();
        self.store
            .set(req.key, req.value)
            .map(|_| SetReply {
                message: "OK".to_string(),
            })
            .map(|reply| Response::new(reply))
            .map_err(|_| Status::unknown("Failed"))
    }
    async fn get(
        &self,
        req: Request<GetRequest>,
    ) -> std::result::Result<Response<GetReply>, Status> {
        let req = req.into_inner();
        self.store
            .get(req.key)
            .map(|value| GetReply {
                message: value.unwrap_or("Key not found".to_string()),
            })
            .map(|reply| Response::new(reply))
            .map_err(|e| Status::unknown(e.to_string()))
    }
    async fn remove(
        &self,
        req: Request<RemoveRequest>,
    ) -> std::result::Result<Response<RemoveReply>, Status> {
        let req = req.into_inner();
        self.store
            .remove(req.key)
            .map(|_| RemoveReply {
                message: "OK".to_string(),
            })
            .map(|reply| Response::new(reply))
            .map_err(|e| Status::unknown(e.to_string()))
    }
}

impl KvsServer {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: EngineKind, _thread_pool: ThreadPoolKind) -> Result<Self> {
        Ok(KvsServer {
            store: Arc::new(store),
        })
    }
    // Run Kvs Server at given Addr
    //     pub fn run(&mut self, addr: SocketAddr) -> Result<()> {
    //         let listener = TcpListener::bind(addr)?;

    //         info!("[Server] Listening on {}", addr);

    //         // accept connections and process them serially
    //         for stream in listener.incoming() {
    //             match stream {
    //                 Ok(stream) => {
    //                     let store = self.store.clone();
    //                     self.thread_pool.spawn(move || {
    //                         handle_request(store, stream).unwrap();
    //                     })
    //                 }
    //                 Err(e) => println!("{}", e),
    //             }
    //         }
    //         Ok(())
    //     }
}

// fn handle_request(store: EngineKind, stream: TcpStream) -> Result<()> {
//     let mut reader = BufReader::new(&stream);
//     let mut writer = BufWriter::new(&stream);
//
//     let mut buf = vec![];
//     let _len = reader.read_until(b'}', &mut buf)?;
//     let request_str = from_utf8(&buf).unwrap();
//
//     let request: Request = serde_json::from_str(request_str)?;
//     let response = process_request(store, request);
//
//     let response_str = serde_json::to_string(&response)?;
//     writer.write(&response_str.as_bytes())?;
//     writer.flush()?;
//
//     info!(
//         "[Server] Received request from {} - Args: {}, Response: {}",
//         stream.local_addr()?,
//         request_str,
//         response_str
//     );
//
//     Ok(())
// }

// fn process_request(store: EngineKind, req: crate::Request) -> crate::Response {
//     use crate::Response;
//     match req.cmd.as_str() {
//         "Get" => match store.get(req.key) {
//             Ok(Some(value)) => Response {
//                 status: "ok".to_string(),
//                 result: Some(value),
//             },
//             Ok(None) => Response {
//                 status: "ok".to_string(),
//                 result: Some("Key not found".to_string()),
//             },
//             Err(_) => Response {
//                 status: "err".to_string(),
//                 result: Some("Something Wrong!".to_string()),
//             },
//         },
//         "Set" => match store.set(req.key, req.value.unwrap()) {
//             Ok(_) => Response {
//                 status: "ok".to_string(),
//                 result: None,
//             },
//             Err(_) => Response {
//                 status: "err".to_string(),
//                 result: Some("Set Error!".to_string()),
//             },
//         },
//         "Remove" => match store.remove(req.key) {
//             Ok(_) => Response {
//                 status: "ok".to_string(),
//                 result: None,
//             },
//             Err(_) => Response {
//                 status: "err".to_string(),
//                 result: Some("Key not found".to_string()),
//             },
//         },
//         _ => Response {
//             status: "err".to_string(),
//             result: Some("Unknown Command!".to_string()),
//         },
//     }
// }
