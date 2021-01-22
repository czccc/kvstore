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

fn client_cli_txn_single_access(engine: &str, addr: &str) {
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
#[test]
fn client_cli_txn_single_access_kvs() {
    client_cli_txn_single_access("kvs", "127.0.0.1:5001");
}

#[test]
fn client_cli_txn_single_access_sled() {
    client_cli_txn_single_access("sled", "127.0.0.1:4001");
}

fn client_cli_txn_pmp_read_predicates(engine: &str, addr: &str) {
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
#[test]
fn client_cli_txn_pmp_read_predicates_kvs() {
    client_cli_txn_pmp_read_predicates("kvs", "127.0.0.1:4002");
}

#[test]
fn client_cli_txn_pmp_read_predicates_sled() {
    client_cli_txn_pmp_read_predicates("sled", "127.0.0.1:4003");
}
