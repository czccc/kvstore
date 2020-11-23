use kvs::{Request, Response};
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream},
    process::exit,
    str::from_utf8,
};
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
            default_value = "127.0.0.1:4000"
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
            default_value = "127.0.0.1:4000"
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
            default_value = "127.0.0.1:4000"
        )]
        addr: SocketAddr,
    },
}

fn client_handle(stream: TcpStream, buf: Request) -> Result<(), std::io::Error> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let method: String = buf.cmd.clone();

    let buf = serde_json::to_string(&buf)?;

    writer.write(buf.as_bytes())?;
    writer.flush()?;
    // println!("Send Ok...");
    let mut buf = vec![];
    reader.read_until(b'}', &mut buf)?;
    // println!("{}: {}", len, from_utf8(&buf).unwrap());
    let response: Response = serde_json::from_str(from_utf8(&buf).unwrap())?;

    match response.status.as_str() {
        "ok" => match response.result {
            Some(value) => println!("{}", value),
            None => (),
        },
        "err" => match method.as_str() {
            "Get" => println!("{}", response.message.unwrap()),
            "Set" => println!("{}", response.message.unwrap()),
            "Remove" => {
                eprintln!("{}", response.message.unwrap());
                exit(1)
            }
            _ => (),
        },
        _ => println!("Unknown Error"),
    }

    Ok(())
}

fn main() -> Result<(), std::io::Error> {
    let opt: Opt = Opt::from_args();
    // println!("{:?}", opt);

    // let stream = TcpStream::connect(opt.cmd);
    // let addr =match opt.cmd {
    //     Command::Get { } => {},
    // }
    match opt.cmd {
        Command::Get { key, addr } => {
            let stream = TcpStream::connect(addr)?;
            let buf = Request {
                cmd: "Get".to_string(),
                key,
                value: None,
            };
            client_handle(stream, buf)?;
        }
        Command::Set { key, value, addr } => {
            let stream = TcpStream::connect(addr)?;
            let buf = Request {
                cmd: "Set".to_string(),
                key,
                value: Some(value),
            };
            client_handle(stream, buf)?;
        }
        Command::Rm { key, addr } => {
            let stream = TcpStream::connect(addr)?;
            let buf = Request {
                cmd: "Remove".to_string(),
                key,
                value: None,
            };
            client_handle(stream, buf)?;
        }
    };
    Ok(())
}
