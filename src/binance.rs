use url;
use futures_channel;
use futures_util::{
    future,
    pin_mut,
    StreamExt,
};
use opentelemetry::{Context, global, Key};
use opentelemetry::trace::{FutureExt, TraceContextExt, Tracer};
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

#[derive(Debug, Deserialize)]
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
    let tracer = global::tracer("run_binance");
    let span = tracer.start("running binance");
    let cx = Context::current_with_span(span);

    let connect_addr = format!(
        "wss://stream.binance.com:9443/ws/{}@depth{}@100ms", symbol, depth,
    );

    let url = url::Url::parse(&connect_addr)?;

    // let (stdin_tx, stdin_rx) = futures_channel::mpsc::unbounded();
    // tokio::spawn(read_stdin(stdin_tx));

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    let (write, read) = ws_stream.split();

    // let stdin_to_ws = stdin_rx.map(Ok).forward(write);
    read.for_each(|message| async {
        let message_data = message.unwrap().into_data();
        let mut binance_parse: serde_json::Result<DepthUpdate> = serde_json::from_slice(
            &message_data,
        );

        match binance_parse {
            Ok(depth_update) => {
                // depth_update.asks.resize(depth);
                // depth_update.bids.resize(depth);
                match depth_update.try_into() {
                    Ok(summary) => {
                        if let Err(err) = summary_tx.send(summary) {
                            cx.span().add_event(
                                "error information to the channel",
                                vec![
                                    Key::new("error").string(format!("{}", err)),
                                ],
                            );
                        }
                    }
                    Err(err) => {
                        cx.span().add_event(
                            "error converting WebSocket data to domain type",
                            vec![
                                Key::new("error").string(format!("{:?}", err)),
                            ],
                        );
                    }
                }
            }
            Err(err) => {
                cx.span().add_event(
                    "error parsing WebSocket data",
                    vec![
                        Key::new("message").string(format!("{:?}", message_data)),
                        Key::new("error").string(format!("{}", err)),
                    ],
                );
            }
        }
    }).with_context(cx.clone()).await;

    // pin_mut!(stdin_to_ws, ws_to_stdout)
    // future::select(stdin_to_ws, ws_to_stdout).await;
    Ok(())
}