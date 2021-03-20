use assert_cmd::prelude::*;
use kvs::preclude::*;
use mpsc::SyncSender;
use std::{
    io::{prelude::*, BufReader, BufWriter},
    process::Command,
    process::{Child, ChildStdin, ChildStdout, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};
use tempfile::TempDir;
use thread::JoinHandle;
use tonic::transport::Channel;

fn open_server(
    engine: &str,
    addrs: Vec<&str>,
    temp_dir: &TempDir,
) -> (SyncSender<()>, JoinHandle<()>) {
    let (sender, receiver) = mpsc::sync_channel::<()>(0);
    let mut addr = vec![];
    for add in addrs {
        addr.push("--addr");
        addr.push(add);
    }
    let mut child = Command::cargo_bin("kvs-server")
        .unwrap()
        .args(&[["--engine", engine, "--server", "raft"].to_vec(), addr].concat())
        .env("RUST_LOG", "warn")
        .current_dir(temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));
    (sender, handle)
}

struct ClientWrapper {
    child: Child,
    reader: BufReader<ChildStdout>,
    writer: BufWriter<ChildStdin>,
}
impl ClientWrapper {
    fn new(addrs: Vec<&str>) -> ClientWrapper {
        let mut addr = vec![];
        for add in addrs {
            addr.push("--addr");
            addr.push(add);
        }
        let mut child = Command::cargo_bin("kvs-client")
            .unwrap()
            .args(&[["txn"].to_vec(), addr].concat())
            .env("RUST_LOG", "warn")
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        thread::sleep(Duration::from_secs(1));

        let stdout = child.stdout.take().expect("Unable get stdout");
        let reader = BufReader::new(stdout);
        let stdin = child.stdin.take().expect("Unable get stdin");
        let writer = BufWriter::new(stdin);

        let mut client = ClientWrapper {
            child,
            reader,
            writer,
        };
        client.begin();
        client
    }
    fn begin(&mut self) {
        let buf = format!("begin\n");
        self.writer.write(buf.as_bytes()).expect("Writer error");
        self.writer.flush().expect("Writer error");

        let mut reader_buf = String::new();
        self.reader.read_line(&mut reader_buf).unwrap();
        assert_eq!("Transaction Started", reader_buf.trim());
    }
    fn set(&mut self, key: &str, value: &str) {
        let buf = format!("set {} {}\n", key, value);
        self.writer.write(buf.as_bytes()).expect("Writer error");
        self.writer.flush().expect("Writer error");
    }
    fn get(&mut self, key: &str, expected: &str) {
        let buf = format!("get {}\n", key);
        self.writer.write(buf.as_bytes()).expect("Writer error");
        self.writer.flush().expect("Writer error");

        let mut reader_buf = String::new();
        self.reader.read_line(&mut reader_buf).unwrap();
        // println!("expected: {}, get: {}", expected, reader_buf);
        assert!(reader_buf.trim().contains(expected.trim()));
    }
    fn commit(&mut self, expected: &str) {
        let buf = format!("commit\n");
        self.writer.write(buf.as_bytes()).expect("Writer error");
        self.writer.flush().expect("Writer error");

        let mut reader_buf = String::new();
        self.reader.read_line(&mut reader_buf).unwrap();
        assert!(reader_buf.trim().contains(expected.trim()));

        self.exit();
    }
    fn exit(&mut self) {
        let buf = format!("exit\n");
        self.writer.write(buf.as_bytes()).expect("Writer error");
        self.writer.flush().expect("Writer error");
        self.child.wait().expect("command wasn't running");
    }
}

#[test]
fn client_cli_txn_invalid() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["txn", "extra"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["txn", "--addr", "invalid-addr"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["txn", "--unknown-flag"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

#[test]
fn client_cli_txn_single_access() {
    let addr = vec!["127.0.0.1:6001", "127.0.0.1:6002", "127.0.0.1:6003"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.get("key1", "Key not found");
        client0.get("key3", "Key not found");
        client0.commit("Transaction Success");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_pmp_read_predicates() {
    let addr = vec!["127.0.0.1:6011", "127.0.0.1:6012", "127.0.0.1:6013"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        client1.get("key3", "Key not found");

        let mut client2 = ClientWrapper::new(addr.clone());
        client2.set("key3", "300");
        client2.commit("Transaction Success");

        client1.get("key3", "Key not found");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_pmp_write_predicates() {
    let addr = vec!["127.0.0.1:6021", "127.0.0.1:6022", "127.0.0.1:6023"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        let mut client2 = ClientWrapper::new(addr.clone());

        client1.set("key1", "200");
        client1.set("key2", "300");
        client1.get("key2", "200");

        client2.set("key2", "400");

        client1.commit("Transaction Success");
        client2.commit("Transaction Failed");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_lost_update() {
    let addr = vec!["127.0.0.1:6031", "127.0.0.1:6032", "127.0.0.1:6033"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        let mut client2 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client2.get("key1", "100");

        client1.set("key1", "101");
        client2.set("key1", "101");

        client1.commit("Transaction Success");
        client2.commit("Transaction Failed");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_read_skew_read_only() {
    let addr = vec!["127.0.0.1:6041", "127.0.0.1:6042", "127.0.0.1:6043"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        let mut client2 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client2.get("key1", "100");
        client2.get("key2", "200");

        client2.set("key1", "101");
        client2.set("key2", "201");
        client2.commit("Transaction Success");

        client1.get("key2", "200");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_read_skew_predicate_dependencies() {
    let addr = vec!["127.0.0.1:6051", "127.0.0.1:6052", "127.0.0.1:6053"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        let mut client2 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client1.get("key2", "200");

        client2.set("key3", "300");
        client2.commit("Transaction Success");

        client1.get("key3", "Key not found");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_read_skew_write_predicate() {
    let addr = vec!["127.0.0.1:6061", "127.0.0.1:6062", "127.0.0.1:6063"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        let mut client2 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client2.get("key1", "100");
        client2.get("key2", "200");

        client2.set("key1", "101");
        client2.set("key2", "201");
        client2.commit("Transaction Success");

        client1.set("key2", "300");
        client1.commit("Transaction Failed");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_write_skew() {
    let addr = vec!["127.0.0.1:6071", "127.0.0.1:6072", "127.0.0.1:6073"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        let mut client2 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client1.get("key2", "200");
        client2.get("key1", "100");
        client2.get("key2", "200");

        client1.set("key1", "101");
        client2.set("key2", "201");
        client1.commit("Transaction Success");
        client2.commit("Transaction Success");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_anti_dependency_cycles() {
    let addr = vec!["127.0.0.1:6081", "127.0.0.1:6082", "127.0.0.1:6083"];
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        let mut client2 = ClientWrapper::new(addr.clone());

        client1.set("key3", "300");
        client2.set("key4", "400");
        client1.commit("Transaction Success");
        client2.commit("Transaction Success");

        let mut client3 = ClientWrapper::new(addr.clone());

        client3.get("key3", "300");
        client3.get("key4", "400");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

struct Proxy {
    addr: String,
    server_addr: String,
    drop_req: bool,
    drop_resp: bool,
    fail_primary: bool,
}

impl Proxy {
    fn build_client(&self) -> KvRpcClient<Channel> {
        let server_addr = format!("http://{}", self.server_addr);
        let channel = Channel::from_shared(server_addr)
            .unwrap()
            .connect_lazy()
            .unwrap();
        KvRpcClient::new(channel)
    }
}

#[tonic::async_trait]
impl KvRpc for Proxy {
    async fn get_timestamp(
        &self,
        request: tonic::Request<TsRequest>,
    ) -> std::result::Result<tonic::Response<TsReply>, tonic::Status> {
        self.build_client().get_timestamp(request).await
    }

    async fn txn_get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> std::result::Result<tonic::Response<GetReply>, tonic::Status> {
        self.build_client().txn_get(request).await
    }

    async fn txn_prewrite(
        &self,
        request: tonic::Request<PrewriteRequest>,
    ) -> std::result::Result<tonic::Response<PrewriteReply>, tonic::Status> {
        self.build_client().txn_prewrite(request).await
    }

    async fn txn_commit(
        &self,
        request: tonic::Request<CommitRequest>,
    ) -> std::result::Result<tonic::Response<CommitReply>, tonic::Status> {
        let request = request.into_inner();
        let req = if self.drop_req {
            if request.is_primary && !self.fail_primary {
                self.build_client()
                    .txn_commit(tonic::Request::new(request))
                    .await
            } else {
                Err(tonic::Status::data_loss("Drop request"))
            }
        } else {
            self.build_client()
                .txn_commit(tonic::Request::new(request))
                .await
        };
        if self.drop_resp {
            Err(tonic::Status::data_loss("Drop response"))
        } else {
            req
        }
    }
}

struct MultiProxy {
    proxys: Vec<Proxy>,
}

impl MultiProxy {
    fn new(
        addr: Vec<&str>,
        server_addr: Vec<&str>,
        drop_req: bool,
        drop_resp: bool,
        fail_primary: bool,
    ) -> Self {
        let proxys = addr
            .into_iter()
            .zip(server_addr.into_iter())
            .map(|(a, s)| (a.to_owned(), s.to_owned()))
            .map(|(addr, server_addr)| Proxy {
                addr,
                server_addr,
                drop_req,
                drop_resp,
                fail_primary,
            })
            .collect();
        Self { proxys }
    }
}

fn proxy_hook(proxys: MultiProxy) -> Vec<JoinHandle<()>> {
    proxys
        .proxys
        .into_iter()
        .map(|proxy| {
            let threaded_rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            std::thread::spawn(move || {
                threaded_rt
                    .block_on(async move {
                        let addr = proxy.addr.clone().parse().unwrap();
                        tonic::transport::Server::builder()
                            .add_service(KvRpcServer::new(proxy))
                            .serve(addr)
                            .await
                            .map_err(|e| KvError::StringError(e.to_string()))
                    })
                    .unwrap();
            })
        })
        .collect()
}

#[test]
fn client_cli_txn_test_proxy_with_nothing_drop() {
    let server_addr = vec!["127.0.0.1:6091", "127.0.0.1:6092", "127.0.0.1:6093"];
    let addr = vec!["127.0.0.1:6101", "127.0.0.1:6102", "127.0.0.1:6103"];
    let proxy = MultiProxy::new(addr.clone(), server_addr.clone(), false, false, false);
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());
        let mut client2 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client1.get("key2", "200");
        client2.get("key1", "100");
        client2.get("key2", "200");

        client1.set("key1", "101");
        client2.set("key2", "201");
        client1.commit("Transaction Success");
        client2.commit("Transaction Success");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_commit_primary_drop_secondary_requests() {
    let server_addr = vec!["127.0.0.1:6111", "127.0.0.1:6112", "127.0.0.1:6113"];
    let addr = vec!["127.0.0.1:6121", "127.0.0.1:6122", "127.0.0.1:6123"];
    let proxy = MultiProxy::new(addr.clone(), server_addr.clone(), true, false, false);
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client1.get("key2", "200");
        client1.get("key3", "300");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_commit_primary_success() {
    let server_addr = vec!["127.0.0.1:6131", "127.0.0.1:6132", "127.0.0.1:6133"];
    let addr = vec!["127.0.0.1:6141", "127.0.0.1:6142", "127.0.0.1:6143"];

    let proxy = MultiProxy::new(addr.clone(), server_addr.clone(), true, false, false);
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client1.get("key2", "200");
        client1.get("key3", "300");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_commit_primary_success_without_response() {
    let server_addr = vec!["127.0.0.1:6151", "127.0.0.1:6152", "127.0.0.1:6153"];
    let addr = vec!["127.0.0.1:6161", "127.0.0.1:6162", "127.0.0.1:6163"];

    let proxy = MultiProxy::new(addr.clone(), server_addr.clone(), false, true, false);
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Failed");

        let mut client1 = ClientWrapper::new(addr.clone());

        client1.get("key1", "100");
        client1.get("key2", "200");
        client1.get("key3", "300");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_commit_primary_fail() {
    let server_addr = vec!["127.0.0.1:6171", "127.0.0.1:6172", "127.0.0.1:6173"];
    let addr = vec!["127.0.0.1:6181", "127.0.0.1:6182", "127.0.0.1:6183"];

    let proxy = MultiProxy::new(addr.clone(), server_addr.clone(), true, false, true);
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Failed");

        let mut client1 = ClientWrapper::new(addr.clone());

        client1.get("key1", "Key not found");
        client1.get("key2", "Key not found");
        client1.get("key3", "Key not found");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_server_crash() {
    let server_addr = vec!["127.0.0.1:6191", "127.0.0.1:6192", "127.0.0.1:6193"];
    let addr = vec!["127.0.0.1:6201", "127.0.0.1:6202", "127.0.0.1:6203"];

    let proxy = MultiProxy::new(addr.clone(), server_addr.clone(), true, false, true);
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr.clone(), &temp_dir);

        let mut client0 = ClientWrapper::new(addr.clone());
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Failed");

        sender.send(()).unwrap();
        handle.join().unwrap();
        let (sender, handle) = open_server(engine, server_addr.clone(), &temp_dir);

        let mut client1 = ClientWrapper::new(addr.clone());

        client1.get("key1", "Key not found");
        client1.get("key2", "Key not found");
        client1.get("key3", "Key not found");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}
