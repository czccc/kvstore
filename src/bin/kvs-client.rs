use kvs::preclude::*;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, process::exit};
use structopt::StructOpt;

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
    match opt.cmd {
        Command::Get { key, addr } => {
            let addr = format!("{}{}", "http://", addr.to_string());
            let mut client = KvRpcClient::connect(addr).await.unwrap();
            let request = tonic::Request::new(GetRequest { key });
            let response = client.get(request).await;
            match response {
                Ok(result) => println!("{}", result.into_inner().message),
                Err(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
            }
        }
        Command::Set { key, value, addr } => {
            let addr = format!("{}{}", "http://", addr.to_string());
            let mut client = KvRpcClient::connect(addr).await.unwrap();
            let request = tonic::Request::new(SetRequest { key, value });
            let response = client.set(request).await;
            match response {
                Ok(_result) => {}
                Err(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
            }
        }
        Command::Rm { key, addr } => {
            let addr = format!("{}{}", "http://", addr.to_string());
            let mut client = KvRpcClient::connect(addr).await.unwrap();
            let request = tonic::Request::new(RemoveRequest { key });
            let response = client.remove(request).await;
            match response {
                Ok(_result) => {}
                Err(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
            }
        }
    };
    Ok(())
}
