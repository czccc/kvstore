// extern crate clap;
// use clap::{App, Arg, SubCommand};

use kvs::{KvStore, Result};
use serde::{Deserialize, Serialize};
use std::{env::current_dir, process::exit};
use structopt::StructOpt;

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

#[derive(Debug, StructOpt, Deserialize, Serialize)]
enum Command {
    #[structopt(about = "Get the string value of a given string key")]
    Get {
        #[structopt(help = "A string key")]
        key: String,
    },
    #[structopt(about = "Set the value of a string key to a string")]
    Set {
        #[structopt(help = "A string key")]
        key: String,
        #[structopt(help = "The string value of the key")]
        value: String,
    },
    #[structopt(about = "Remove a given key")]
    Rm {
        #[structopt(help = "A string key")]
        key: String,
    },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    match opt.cmd {
        Command::Get { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.get(key)? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        Command::Set { key, value } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key, value)?;
        }
        Command::Rm { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key) {
                Ok(_) => exit(0),
                Err(e) => {
                    println!("{}", e);
                    exit(1)
                }
            }
        }
    };
    Ok(())
}
