use assert_cmd::prelude::*;
use kvs::*;
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

fn open_server(engine: &str, addr: &str, temp_dir: &TempDir) -> (SyncSender<()>, JoinHandle<()>) {
    let (sender, receiver) = mpsc::sync_channel::<()>(0);
    let mut child = Command::cargo_bin("kvs-server")
        .unwrap()
        .args(&["--engine", engine, "--addr", addr])
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
    fn new(addr: &str) -> ClientWrapper {
        let mut child = Command::cargo_bin("kvs-client")
            .unwrap()
            .args(&["txn", "--addr", addr])
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
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
        assert_eq!(expected.trim(), reader_buf.trim());
    }
    fn commit(&mut self, expected: &str) {
        let buf = format!("commit\n");
        self.writer.write(buf.as_bytes()).expect("Writer error");
        self.writer.flush().expect("Writer error");

        let mut reader_buf = String::new();
        self.reader.read_line(&mut reader_buf).unwrap();
        assert_eq!(expected, reader_buf.trim());

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
    let addr = "127.0.0.1:4001";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
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
    let addr = "127.0.0.1:4002";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        client1.get("key3", "Key not found");

        let mut client2 = ClientWrapper::new(addr);
        client2.set("key3", "300");
        client2.commit("Transaction Success");

        client1.get("key3", "Key not found");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_pmp_write_predicates() {
    let addr = "127.0.0.1:4003";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        let mut client2 = ClientWrapper::new(addr);

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
    let addr = "127.0.0.1:4004";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        let mut client2 = ClientWrapper::new(addr);

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
    let addr = "127.0.0.1:4005";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        let mut client2 = ClientWrapper::new(addr);

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
    let addr = "127.0.0.1:4006";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        let mut client2 = ClientWrapper::new(addr);

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
    let addr = "127.0.0.1:4007";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        let mut client2 = ClientWrapper::new(addr);

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
    let addr = "127.0.0.1:4008";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        let mut client2 = ClientWrapper::new(addr);

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
    let addr = "127.0.0.1:4009";
    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        let mut client2 = ClientWrapper::new(addr);

        client1.set("key3", "300");
        client2.set("key4", "400");
        client1.commit("Transaction Success");
        client2.commit("Transaction Success");

        let mut client3 = ClientWrapper::new(addr);

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

fn proxy_hook(proxy: Proxy) -> JoinHandle<()> {
    thread::spawn(move || {
        let listener = std::net::TcpListener::bind(proxy.addr).unwrap();
        // accept connections and process them serially
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let mut reader = BufReader::new(&stream);
                    let mut writer = BufWriter::new(&stream);

                    let mut buf = vec![];
                    let _len = reader.read_until(b'}', &mut buf).unwrap();
                    let request_str = std::str::from_utf8(&buf).unwrap();

                    let request: Request = serde_json::from_str(request_str).unwrap();

                    let response = if request.cmd == "Commit"
                        && proxy.drop_req
                        && !(request.key == request.primary && !proxy.fail_primary)
                    {
                        Response {
                            status: "err".to_string(),
                            result: Some("Request Timeout!".to_string()),
                        }
                    } else {
                        let stream = std::net::TcpStream::connect(&proxy.server_addr).unwrap();
                        let mut s_reader = BufReader::new(&stream);
                        let mut s_writer = BufWriter::new(&stream);

                        let buf = serde_json::to_string(&request).unwrap();
                        s_writer.write(buf.as_bytes()).unwrap();
                        s_writer.flush().unwrap();

                        let mut buf = String::new();
                        s_reader.read_line(&mut buf).unwrap();
                        serde_json::from_str(&buf).unwrap()
                    };
                    let response = if request.cmd == "Commit" && proxy.drop_resp {
                        Response {
                            status: "err".to_string(),
                            result: Some("Response Timeout!".to_string()),
                        }
                    } else {
                        response
                    };
                    let response_str = serde_json::to_string(&response).unwrap();
                    writer.write(&response_str.as_bytes()).unwrap();
                    writer.flush().unwrap();
                }
                Err(e) => println!("{}", e),
            }
        }
    })
}

#[test]
fn client_cli_txn_test_proxy_with_nothing_drop() {
    let server_addr = "127.0.0.1:4010";
    let addr = "127.0.0.1:4011";
    let proxy = Proxy {
        addr: addr.to_string(),
        server_addr: server_addr.to_string(),
        drop_req: false,
        drop_resp: false,
        fail_primary: false,
    };
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);
        let mut client2 = ClientWrapper::new(addr);

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
    let server_addr = "127.0.0.1:4012";
    let addr = "127.0.0.1:4013";
    let proxy = Proxy {
        addr: addr.to_string(),
        server_addr: server_addr.to_string(),
        drop_req: true,
        drop_resp: false,
        fail_primary: false,
    };
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);

        client1.get("key1", "100");
        client1.get("key2", "200");
        client1.get("key3", "300");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_commit_primary_success() {
    let server_addr = "127.0.0.1:4014";
    let addr = "127.0.0.1:4015";
    let proxy = Proxy {
        addr: addr.to_string(),
        server_addr: server_addr.to_string(),
        drop_req: true,
        drop_resp: false,
        fail_primary: false,
    };
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Success");

        let mut client1 = ClientWrapper::new(addr);

        client1.get("key1", "100");
        client1.get("key2", "200");
        client1.get("key3", "300");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_commit_primary_success_without_response() {
    let server_addr = "127.0.0.1:4016";
    let addr = "127.0.0.1:4017";
    let proxy = Proxy {
        addr: addr.to_string(),
        server_addr: server_addr.to_string(),
        drop_req: false,
        drop_resp: true,
        fail_primary: false,
    };
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Failed");

        let mut client1 = ClientWrapper::new(addr);

        client1.get("key1", "100");
        client1.get("key2", "200");
        client1.get("key3", "300");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_commit_primary_fail() {
    let server_addr = "127.0.0.1:4018";
    let addr = "127.0.0.1:4019";
    let proxy = Proxy {
        addr: addr.to_string(),
        server_addr: server_addr.to_string(),
        drop_req: true,
        drop_resp: false,
        fail_primary: true,
    };
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Failed");

        let mut client1 = ClientWrapper::new(addr);

        client1.get("key1", "Key not found");
        client1.get("key2", "Key not found");
        client1.get("key3", "Key not found");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}

#[test]
fn client_cli_txn_server_crash() {
    let server_addr = "127.0.0.1:4020";
    let addr = "127.0.0.1:4021";
    let proxy = Proxy {
        addr: addr.to_string(),
        server_addr: server_addr.to_string(),
        drop_req: true,
        drop_resp: false,
        fail_primary: true,
    };
    let _proxy_handle = proxy_hook(proxy);
    thread::sleep(Duration::from_secs(1));

    for engine in vec!["kvs", "sled"] {
        let temp_dir = TempDir::new().unwrap();
        let (sender, handle) = open_server(engine, server_addr, &temp_dir);

        let mut client0 = ClientWrapper::new(addr);
        client0.set("key1", "100");
        client0.set("key2", "200");
        client0.set("key3", "300");
        client0.commit("Transaction Failed");

        sender.send(()).unwrap();
        handle.join().unwrap();
        let (sender, handle) = open_server(engine, server_addr, &temp_dir);

        let mut client1 = ClientWrapper::new(addr);

        client1.get("key1", "Key not found");
        client1.get("key2", "Key not found");
        client1.get("key3", "Key not found");

        sender.send(()).unwrap();
        handle.join().unwrap();
    }
}
