extern crate mbooks;

use clap::{
    arg,
    Parser,
    Subcommand,
};
use mbooks::{
    client::run_client,
    server::run_server,
    types::Symbol,
};
use opentelemetry::{
    global,
    sdk::trace as sdktrace,
    trace::TraceError,
};
use slog::{
    Drain,
    info,
    Logger,
    o,
};
use tokio::{
    signal,
    sync::broadcast,
};

#[derive(Clone, Subcommand)]
pub enum Command {
    /// Runs the server
    Server {
        /// Address for the server.
        #[arg(short, long, default_value = "[::1]:50501")]
        address: String,
        /// The depth of the book
        #[arg(short, long, default_value = "10")]
        depth: usize,
        /// The symbol to be pulled from the websocket.
        #[arg(short, long, default_value = "eth/btc")]
        symbol: String,
    },
    /// Runs the client
    Client {
        /// Address of the server to connect to.
        #[arg(short, long, default_value = "http://[::1]:50501")]
        address: String,
    },
}

#[derive(Clone, Parser)]
#[command(author = "Rodolfo Araujo", version, about = "Orderbook merger CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

fn init_tracer() -> Result<sdktrace::Tracer, TraceError> {
    opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("mbooks")
        .install_batch(opentelemetry::runtime::Tokio)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let logger = Logger::root(
        slog_term::FullFormat::new(plain)
            .build().fuse(), o!(),
    );
    let _tracer = init_tracer()?;
    let (shutdown_sender, mut shutdown_receiver) = broadcast::channel(10);

    let log = logger.clone();
    let spawn_shutdown_sender = shutdown_sender.clone();
    let mut spawn_shutdown_receiver = shutdown_sender.subscribe();
    tokio::spawn(async move {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!(log, "got kill signal, starting shutdown");
                spawn_shutdown_sender.send("got kill signal, starting shutdown".to_string())
                    .expect("problem sending shutdown message");
            },
            _ = spawn_shutdown_receiver.recv() => {
                info!(log, "starting shutdown");
            },
        }
        info!(log, "end of spawn signal listener");
    });

    let mut receiver = shutdown_sender.subscribe();
    match Cli::parse().command.clone() {
        Command::Server { address, symbol, depth, .. } => {
            let symbol = Symbol::try_from(symbol)?;
            run_server(
                logger.clone(), shutdown_sender.clone(),
                address, symbol, depth,
            ).await?;
        }
        Command::Client { address, .. } => {
            run_client(logger.clone(), &mut receiver, address).await?;
        }
    };

    drop(shutdown_sender); // Not necessary since it was moved

    // Waiting for all the services to shut down
    let _ = shutdown_receiver.recv().await;

    global::shutdown_tracer_provider();
    Ok(())
}
