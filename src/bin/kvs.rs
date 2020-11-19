// extern crate clap;
// use clap::{App, Arg, SubCommand};

use std::process::exit;
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

#[derive(Debug, StructOpt)]
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


fn main() {
    let opt = Opt::from_args();
    match opt.cmd {
        Command::Get { key: _ } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Command::Set { key: _, value: _ } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Command::Rm { key: _ } => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
    // let matches = App::new(env!("CARGO_PKG_NAME"))
    //     .version(env!("CARGO_PKG_VERSION"))
    //     .author(env!("CARGO_PKG_AUTHORS"))
    //     .about(env!("CARGO_PKG_DESCRIPTION"))
    //     .subcommand(
    //         SubCommand::with_name("get")
    //             .about("Get the string value of a given string key")
    //             .arg(Arg::with_name("key").help("A string key")),
    //     )
    //     .subcommand(
    //         SubCommand::with_name("set")
    //             .about("Set the value of a string key to a string")
    //             .arg(Arg::with_name("key").help("A string key"))
    //             .arg(Arg::with_name("value").help("The string value of the key")),
    //     )
    //     .subcommand(
    //         SubCommand::with_name("rm")
    //             .about("Remove a given key")
    //             .arg(Arg::with_name("key").help("A string key")),
    //     )
    //     .get_matches();
    // match matches.subcommand() {
    //     ("set", Some(_matches)) => {
    //         eprintln!("unimplemented");
    //         exit(1);
    //     }
    //     ("get", Some(_matches)) => {
    //         eprintln!("unimplemented");
    //         exit(1);
    //     }
    //     ("rm", Some(_matches)) => {
    //         eprintln!("unimplemented");
    //         exit(1);
    //     }
    //     _ => unreachable!(),
    // }
}
