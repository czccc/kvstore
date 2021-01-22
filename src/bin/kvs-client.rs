// #[macro_use]
// extern crate log;

use kvs::{KvsClient, Result};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, BufRead},
    net::SocketAddr,
    process::exit,
};
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
    #[structopt(about = "Start a transaction")]
    Txn {
        #[structopt(
            name = "IP-PORT",
            short = "a",
            long = "addr",
            default_value = DEFAULT_ADDR
        )]
        addr: SocketAddr,
    },
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let opt: Opt = Opt::from_args();

    match opt.cmd {
        Command::Get { key, addr } => {
            let mut client = KvsClient::new(addr);
            match client.get(key) {
                Ok(result) => println!("{}", result),
                Err(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
            }
        }
        Command::Set { key, value, addr } => {
            let mut client = KvsClient::new(addr);
            if let Err(e) = client.set(key, value) {
                eprintln!("{}", e);
                exit(1);
            }
        }
        Command::Rm { key, addr } => {
            let mut client = KvsClient::new(addr);
            if let Err(e) = client.remove(key) {
                eprintln!("{}", e);
                exit(1);
            }
        }
        Command::Txn { addr } => {
            let mut client = KvsClient::new(addr);
            client.txn_start()?;

            let stdin = io::stdin();
            let mut handle = stdin.lock();

            loop {
                let mut input = String::new();
                if handle.read_line(&mut input)? == 0 {
                    break;
                }
                let args: Vec<&str> = input.split_ascii_whitespace().collect();
                match parse_txn_args(args) {
                    TxnArgs::Get(k) => {
                        let value = client.txn_get(k)?;
                        println!("{}", value);
                    }
                    TxnArgs::Set(k, v) => {
                        client.txn_set(k, v)?;
                    }
                    TxnArgs::Commit => {
                        if client.txn_commit()? {
                            println!("Transaction Success");
                        } else {
                            println!("Transaction Failed");
                        }
                        exit(0);
                    }
                    TxnArgs::Unknown => {
                        exit(1);
                    }
                };
            }
        }
    };
    log::info!("Client exit without commit!");
    Ok(())
}

enum TxnArgs {
    Get(String),
    Set(String, String),
    Commit,
    Unknown,
}

fn parse_txn_args(args: Vec<&str>) -> TxnArgs {
    if args.len() == 2 && args[0] == "get" {
        TxnArgs::Get(args[1].to_string())
    } else if args.len() == 3 && args[0] == "set" {
        TxnArgs::Set(args[1].to_string(), args[2].to_string())
    } else if args.len() == 1 && args[0] == "commit" {
        TxnArgs::Commit
    } else {
        log::error!("Unknown args {:?}", args);
        TxnArgs::Unknown
    }
}
