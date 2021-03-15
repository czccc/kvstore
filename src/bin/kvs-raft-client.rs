use kvs::{preclude::*, RaftConfig};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, process::exit};
use structopt::StructOpt;
use tonic::{Code, Request};

const DEFAULT_ADDR: &str = "127.0.0.1:4000";

#[derive(Debug, StructOpt)]
#[structopt(
    name = env!("CARGO_PKG_NAME"), 
    about = env!("CARGO_PKG_DESCRIPTION"), 
    version = env!("CARGO_PKG_VERSION"), 
    author = env!("CARGO_PKG_AUTHORS")
)]
struct Opt {
    #[structopt(subcommand)] // Note that we mark a field as a subcommand
    cmd: Command,
}

#[derive(Debug, StructOpt, Serialize, Deserialize)]
enum Command {
    #[structopt(about = "Get the string value of a given string key")]
    Get {
        #[structopt(help = "A string key")]
        key: String,
        #[structopt(
            name = "IP-PORT",
            short = "a",
            long = "addr",
            default_value = DEFAULT_ADDR
        )]
        addr: SocketAddr,
    },
    #[structopt(about = "Set the value of a string key to a string")]
    Set {
        #[structopt(help = "A string key")]
        key: String,
        #[structopt(help = "The string value of the key")]
        value: String,
        #[structopt(
            name = "IP-PORT",
            short = "a",
            long = "addr",
            default_value = DEFAULT_ADDR
        )]
        addr: SocketAddr,
    },
    #[structopt(about = "Remove a given key")]
    Rm {
        #[structopt(help = "A string key")]
        key: String,
        #[structopt(
            name = "IP-PORT",
            short = "a",
            long = "addr",
            default_value = DEFAULT_ADDR
        )]
        addr: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Opt = Opt::from_args();

    let mut config = RaftConfig::new();
    config.add_raft_node("127.0.0.1:5001".parse().unwrap(), None);
    config.add_raft_node("127.0.0.1:5002".parse().unwrap(), None);
    config.add_raft_node("127.0.0.1:5003".parse().unwrap(), None);
    let clients = config.build_kv_raft_clients();

    match opt.cmd {
        Command::Get { key, addr } => {
            let name = addr.to_string();
            let seq = 3;
            let request = GetRequest { key, name, seq };
            loop {
                for mut client in clients.clone() {
                    let request = Request::new(request.clone());
                    let response = client.get(request).await;
                    match response {
                        Ok(result) => {
                            println!("{}", result.into_inner().message);
                            exit(0)
                        }
                        Err(e) if e.code() == Code::PermissionDenied => {
                            continue;
                        }
                        Err(e) => {
                            eprintln!("{}", e);
                            exit(1);
                        }
                    }
                }
            }
        }
        Command::Set { key, value, addr } => {
            let name = addr.to_string();
            let seq = 2;
            let request = SetRequest {
                key,
                value,
                name,
                seq,
            };
            loop {
                for mut client in clients.clone() {
                    let request = Request::new(request.clone());
                    let response = client.set(request).await;
                    match response {
                        Ok(result) => {
                            println!("{}", result.into_inner().message);
                            exit(0)
                        }
                        Err(e) if e.code() == Code::PermissionDenied => {
                            continue;
                        }
                        Err(e) => {
                            eprintln!("{}", e);
                            exit(1);
                        }
                    }
                }
            }
        }
        Command::Rm { key, addr } => {
            let name = addr.to_string();
            let seq = 1;
            let request = RemoveRequest { key, name, seq };
            loop {
                for mut client in clients.clone() {
                    let request = Request::new(request.clone());
                    let response = client.remove(request).await;
                    match response {
                        Ok(result) => {
                            println!("{}", result.into_inner().message);
                            exit(0)
                        }
                        Err(e) if e.code() == Code::PermissionDenied => {
                            continue;
                        }
                        Err(e) => {
                            eprintln!("{}", e);
                            exit(1);
                        }
                    }
                }
            }
        }
    };
}
