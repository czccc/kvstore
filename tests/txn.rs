use assert_cmd::prelude::*;
use std::{
    io::{prelude::*, BufReader, BufWriter},
    process::Command,
    process::{ChildStdin, ChildStdout, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};
use tempfile::TempDir;

fn assert_get(reader: &mut BufReader<ChildStdout>, res: &str) {
    let mut reader_buf = String::new();
    reader.read_line(&mut reader_buf).unwrap();
    // println!("{}", reader_buf.trim_end());
    assert_eq!(res.trim(), reader_buf.trim());
}
fn assert_write(writer: &mut BufWriter<ChildStdin>, res: &str) {
    writer
        .write((res.to_string() + "\n").as_bytes())
        .expect("Writer error");
    writer.flush().expect("Writer error");
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
fn client_cli_txn_access() {
    let engine = "kvs";
    let addr = "127.0.0.1:4002";

    let (sender, receiver) = mpsc::sync_channel(0);
    let temp_dir = TempDir::new().unwrap();
    let mut server = Command::cargo_bin("kvs-server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(2));

    let mut child = Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["txn", "--addr", addr])
        .current_dir(&temp_dir)
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();

    let stdout = child.stdout.take().expect("Unable get stdout");
    let mut reader = BufReader::new(stdout);
    let stdin = child.stdin.take().expect("Unable get stdout");
    let mut writer = BufWriter::new(stdin);

    assert_write(&mut writer, "set key1 value1");
    assert_write(&mut writer, "set key2 value2");

    assert_write(&mut writer, "get key1");
    assert_get(&mut reader, "Placehold");

    assert_write(&mut writer, "commit");
    assert_get(&mut reader, "Transaction Success");

    child.wait().expect("command wasn't running");

    sender.send(()).unwrap();
    handle.join().unwrap();
}
