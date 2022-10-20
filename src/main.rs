extern crate slog;
extern crate slog_term;

mod types;
mod binance;
mod bitstamp;
mod orderbook;
mod client;
mod server;
mod merger;

use clap::{
    arg,
    Parser,
    Subcommand,
};
use crate::{
    client::run_client,
    server::run_server,
    types::Symbol,
};
use opentelemetry::{
    global,
    sdk::trace as sdktrace,
    trace::TraceError,
};
use serde::Serialize;
use slog::{
    Drain,
    Fuse,
    Key,
    Logger,
    o,
    OwnedKVList,
    Record,
    Serializer,
};
use std::{fmt, result};

fn init_tracer() -> Result<sdktrace::Tracer, TraceError> {
    opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("mbooks")
        .install_batch(opentelemetry::runtime::Tokio)
}

#[derive(Subcommand)]
enum Commands {
    /// Runs the server
    Server {
        /// The server port
        #[arg(short, long, default_value = "50501")]
        port: u16,
        /// The depth of the book
        #[arg(short, long, default_value = "10")]
        depth: usize,
        /// The symbol to be pulled from the websocket.
        #[arg(short, long, default_value = "eth/btc")]
        symbol: String,
    },
    /// Runs the client
    Client {
        /// lists test values
        #[arg(short, long, default_value = "50501")]
        port: u16,
    },
}

#[derive(Parser)]
#[command(author = "Rodolfo Araujo", version, about = "Orderbook merger CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let logger = Logger::root(
        slog_term::FullFormat::new(plain)
            .build().fuse(), o!(),
    );
    let _ = init_tracer()?;
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { port, symbol, depth, .. } => {
            // run_server(port).with_context(cx).await?;
            let symbol = Symbol::try_from(symbol)?;
            run_server(logger.clone(), port, symbol, depth).await?;
        }
        Commands::Client { port, .. } => {
            // run_client(port).with_context(cx).await?;
            run_client(logger.clone(), port).await?;
        }
    }

    global::shutdown_tracer_provider();
    Ok(())
}
