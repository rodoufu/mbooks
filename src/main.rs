mod types;
mod binance;
mod bitstamp;
mod orderbook;
mod client;
mod server;

use clap::{
    arg,
    Command,
};
use clap::{Parser, Subcommand};
use crate::{
    client::run_client,
    server::run_server,
};
use opentelemetry::trace::TraceError;
use opentelemetry::{
    global,
    sdk::trace as sdktrace,
};
use crate::types::Symbol;

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
        depth: u16,
        /// lists test values
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
    let tracer = init_tracer()?;
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { port, symbol, depth, .. } => {
            // run_server(port).with_context(cx).await?;
            let symbol = Symbol::try_from(symbol)?;
            run_server(port, symbol, depth).await?;
        }
        Commands::Client { port, .. } => {
            // run_client(port).with_context(cx).await?;
            run_client(port).await?;
        }
        _ => {
            // println!("verbose: {:?}", cli.verbose);
        }
    }

    global::shutdown_tracer_provider();
    Ok(())
}
