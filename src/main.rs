mod orderbook;
mod server;
mod client;

use crate::{
    client::run_client,
    server::run_server,
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        println!("calling server");
        run_server().await?;
    } else {
        println!("calling client");
        run_client().await?;
    }

    Ok(())
}
