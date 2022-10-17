mod orderbook;
mod client;
mod server;

use clap::{
    arg,
    Command,
};
use crate::{
    client::run_client,
    server::run_server,
};
use opentelemetry::trace::TraceError;
use opentelemetry::{
    global,
    sdk::trace as sdktrace,
};

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

fn init_tracer() -> Result<sdktrace::Tracer, TraceError> {
    opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("mbooks")
        .install_batch(opentelemetry::runtime::Tokio)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tracer = init_tracer()?;
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("server", sub_matches)) => {
            let port_str = sub_matches.get_one::<String>("PORT").expect("port is required");
            if let Ok(port) = port_str.parse::<u16>() {
                // run_server(port).with_context(cx).await?;
                run_server(port).await?;
            } else {
                println!("Invalid port: {}", port_str);
            }
        }
        Some(("client", sub_matches)) => {
            let port_str = sub_matches.get_one::<String>("PORT").expect("port is required");
            if let Ok(port) = port_str.parse::<u16>() {
                // run_client(port).with_context(cx).await?;
                run_client(port).await?;
            } else {
                println!("Invalid port: {}", port_str);
            }
        }
        _ => {
            println!("unexpected");
        }
    }

    global::shutdown_tracer_provider();
    Ok(())
}
