use std::env;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["proto/kvs.proto"], &["proto"])
        .unwrap();
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["proto/helloworld.proto"], &["proto"])
        .unwrap();
}
