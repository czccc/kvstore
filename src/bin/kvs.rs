extern crate clap;
use clap::{Arg, App, SubCommand};

use std::process::exit;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
                            .version(env!("CARGO_PKG_VERSION"))
                            .author(env!("CARGO_PKG_AUTHORS"))
                            .about(env!("CARGO_PKG_DESCRIPTION"))
                            .subcommand(
                                SubCommand::with_name("get")
                                    .about("Get the string value of a given string key")
                                    .arg(Arg::with_name("key").help("A string key")),
                            )
                            .subcommand(
                                SubCommand::with_name("set")
                                    .about("Set the value of a string key to a string")
                                    .arg(Arg::with_name("key").help("A string key"))
                                    .arg(Arg::with_name("value").help("The string value of the key")),
                            )
                            .subcommand(
                                SubCommand::with_name("rm")
                                    .about("Remove a given key")
                                    .arg(Arg::with_name("key").help("A string key")),
                            )
                            .get_matches();
    match matches.subcommand() {
        ("set", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("get", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("rm", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => unreachable!(),
    }
}
