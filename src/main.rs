mod orderbook;
mod server;
mod client;

use clap::{
    arg,
    Command,
};
use crate::{
    client::run_client,
    server::run_server,
};
use std::env;

fn cli() -> Command {
    Command::new("mbooks")
        .about("Orderbook merger CLI")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("server")
                .about("Starts an instance of the server")
                .arg(arg!(<PORT> "The server port"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("client")
                .about("Starts an instance of the client")
                .arg(arg!(<PORT> "The server port for the client to connect"))
                .arg_required_else_help(true),
        )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("server", sub_matches)) => {
            let port_str = sub_matches.get_one::<String>("PORT").expect("required");
            if let Ok(port) = port_str.parse::<u16>() {
                run_server(port).await?;
            } else {
                println!("Invalid port: {}", port_str);
            }
        }
        Some(("client", sub_matches)) => {
            let port_str = sub_matches.get_one::<String>("PORT").expect("required");
            if let Ok(port) = port_str.parse::<u16>() {
                run_client(port).await?;
            } else {
                println!("Invalid port: {}", port_str);
            }
        }
        _ => {
            println!("unexpected");
        }
    }

    Ok(())
}
