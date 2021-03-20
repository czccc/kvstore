use kvs::preclude::*;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, process::exit};
use structopt::StructOpt;

#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref DEFAULT_ADDRS: Vec<SocketAddr> = vec![
        "127.0.0.1:5001".parse().unwrap(),
        "127.0.0.1:5002".parse().unwrap(),
        "127.0.0.1:5003".parse().unwrap()
    ];
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = env!("CARGO_PKG_NAME"), 
    about = env!("CARGO_PKG_DESCRIPTION"), 
    version = env!("CARGO_PKG_VERSION"), 
    author = env!("CARGO_PKG_AUTHORS")
)]
struct Opt {
    #[structopt(flatten)] // Note that we mark a field as a subcommand
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
            // default_value = DEFAULT_ADDR,
            // parse(try_from_str = parse_str_to_vec)
        )]
        addrs: Vec<SocketAddr>,
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
            // default_value = DEFAULT_ADDR,
            // parse(try_from_str = parse_str_to_vec)
        )]
        addrs: Vec<SocketAddr>,
    },
    #[structopt(about = "Remove a given key")]
    Rm {
        #[structopt(help = "A string key")]
        key: String,
        #[structopt(
            name = "IP-PORT",
            short = "a",
            long = "addr",
            // default_value = DEFAULT_ADDR,
            // parse(try_from_str = parse_str_to_vec)
        )]
        addrs: Vec<SocketAddr>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Opt = Opt::from_args();
    println!("{:?}", opt);

    match opt.cmd {
        Command::Get { key, mut addrs } => {
            if addrs.is_empty() {
                addrs = (*DEFAULT_ADDRS).to_owned();
            }
            let mut client = KvsClient::builder().add_batch_nodes(addrs).build();
            match client.get(key).await {
                Ok(value) => println!("{}", value),
                Err(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
            }
        }
        Command::Set {
            key,
            value,
            mut addrs,
        } => {
            if addrs.is_empty() {
                addrs = (*DEFAULT_ADDRS).to_owned();
            }
            let mut client = KvsClient::builder().add_batch_nodes(addrs).build();
            match client.set(key, value).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
            }
        }
        Command::Rm { key, mut addrs } => {
            if addrs.is_empty() {
                addrs = (*DEFAULT_ADDRS).to_owned();
            }
            let mut client = KvsClient::builder().add_batch_nodes(addrs).build();
            match client.remove(key).await {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
            }
        }
    };
    Ok(())
}
