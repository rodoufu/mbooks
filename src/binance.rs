use url;
use futures_channel;
use futures_util::{
    future,
    pin_mut,
    StreamExt,
};
use serde_derive::{
    Deserialize,
    Serialize,
};
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    sync::mpsc::UnboundedSender,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::Message,
};
use crate::types::{Level, Summary, WebsocketError};

#[derive(Deserialize)]
struct DepthUpdate {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

impl TryInto<Summary> for DepthUpdate {
    type Error = WebsocketError;

    fn try_into(self) -> Result<Summary, Self::Error> {
        let mut bids = Vec::with_capacity(self.bids.len());
        for bid in &self.bids {
            bids.push(Level {
                exchange: "binance".to_string(),
                price: bid[0].parse::<f64>().map_err(WebsocketError::ParseError)?,
                quantity: bid[1].parse::<f64>().map_err(WebsocketError::ParseError)?,
            });
        }

        let mut asks = Vec::with_capacity(self.asks.len());
        for ask in &self.asks {
            asks.push(Level {
                exchange: "binance".to_string(),
                price: ask[0].parse::<f64>().map_err(WebsocketError::ParseError)?,
                quantity: ask[1].parse::<f64>().map_err(WebsocketError::ParseError)?,
            });
        }

        Ok(Summary {
            bids,
            asks,
        })
    }
}

pub async fn run_binance(
    summary_tx: UnboundedSender<Summary>,
    symbol: &str, depth: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let connect_addr = format!(
        "wss://stream.binance.com:9443/ws/{}@depth{}@100ms", symbol, depth,
    );

    let url = url::Url::parse(&connect_addr).unwrap();

    // let (stdin_tx, stdin_rx) = futures_channel::mpsc::unbounded();
    // tokio::spawn(read_stdin(stdin_tx));

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    let (write, read) = ws_stream.split();

    // let stdin_to_ws = stdin_rx.map(Ok).forward(write);
    let ws_to_stdout = {
        read.for_each(|message| async {
            let mut binance_parse: serde_json::Result<DepthUpdate> = serde_json::from_slice(
                &message.unwrap().into_data(),
            );

            match binance_parse {
                Ok(mut depth_update) => {
                    // depth_update.asks.resize(depth);
                    // depth_update.bids.resize(depth);
                    let summary = depth_update.try_into().ok().unwrap();
                    summary_tx.send(summary).unwrap();
                }
                Err(err) => {

                }
            }
        })
    };

    // pin_mut!(stdin_to_ws, ws_to_stdout);
    // future::select(stdin_to_ws, ws_to_stdout).await;
    ws_to_stdout.await;
    Ok(())
}