use assert_cmd::prelude::*;
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

        ClientWrapper {
            child,
            reader,
            writer,
        }
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
