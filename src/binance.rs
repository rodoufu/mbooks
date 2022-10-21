use crate::types::{
    Level,
    Symbol,
    Summary,
    WebsocketError,
};
use futures_util::StreamExt;
use opentelemetry::{
    Context,
    global,
    Key,
    trace::{
        FutureExt,
        TraceContextExt,
        Tracer,
    },
};
use serde_derive::Deserialize;
use slog::{
    debug,
    Logger,
    info,
    o,
    error,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::connect_async;

#[derive(Debug, Deserialize)]
struct DepthSnapshot {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

impl TryInto<Summary> for DepthSnapshot {
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

fn symbol_to_string(symbol: &Symbol) -> String {
    format!("{}{}", symbol.base.to_string(), symbol.quote.to_string()).to_lowercase()
}

pub async fn run_binance(
    log: Logger,
    summary_tx: UnboundedSender<Summary>,
    symbol: &Symbol, depth: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let tracer = global::tracer("run_binance");
    let span = tracer.start("running binance");
    let cx = Context::current_with_span(span);
    let log = log.new(o!("exchange" => "binance", "symbol" => format!("{:?}", symbol)));
    info!(log, "running binance");

    let connect_addr = format!(
        "wss://stream.binance.com:9443/ws/{}@depth{}@100ms", symbol_to_string(symbol), depth,
    );

    let url = url::Url::parse(&connect_addr)?;
    info!(log, "binance url"; "url" => format!("{:?}", url));


    let (ws_stream, _) = connect_async(url)
        .with_context(cx.clone())
        .await.expect("Failed to connect");
    info!(log, "WebSocket handshake has been successfully completed");

    let (_, read) = ws_stream.split();

    read.for_each(|message| async {
        debug!(log, "websocket got message");
        match message {
            Ok(message_data) => {
                let message_data = message_data.into_data();
                let binance_parse: serde_json::Result<DepthSnapshot> = serde_json::from_slice(
                    &message_data,
                );

                match binance_parse {
                    Ok(depth_update) => {
                        match depth_update.try_into() {
                            Ok(summary) => {
                                if let Err(err) = summary_tx.send(summary) {
                                    error!(
                                        log, "error sending information to the channel";
                                        "error" => format!("{}", err)
                                    );
                                    cx.span().add_event(
                                        "error sending information to the channel",
                                        vec![
                                            Key::new("error").string(format!("{}", err)),
                                        ],
                                    );
                                }
                            }
                            Err(err) => {
                                error!(
                                    log, "error converting WebSocket data to domain type";
                                    "error" => format!("{}", err)
                                );
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
                        error!(log, "error parsing WebSocket data"; "error" => format!("{}", err));
                        cx.span().add_event(
                            "error parsing WebSocket data",
                            vec![
                                Key::new("message").string(format!("{:?}", message_data)),
                                Key::new("error").string(format!("{}", err)),
                            ],
                        );
                    }
                }
            }
            Err(err) => {
                error!(log, "problem fetching message"; "error" => format!("{}", err));
            }
        }
    }).with_context(cx.clone()).await;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{
        binance::{
            symbol_to_string,
            DepthSnapshot,
        },
        types::{
            Asset,
            Symbol,
        },
    };

    #[test]
    fn should_parse_data() {
        // Given
        let msg = r#"{"lastUpdateId":6062044077,"bids":[["0.06754400","31.99050000"],["0.06754300","4.60890000"]],"asks":[["0.06754500","27.06160000"],["0.06754600","5.45080000"],["0.06754700","0.03340000"]]}"#;

        // When
        let resp: DepthSnapshot = serde_json::from_str(msg).unwrap();

        // Then
        assert_eq!(2, resp.bids.len());
        assert_eq!(3, resp.asks.len());
    }

    #[test]
    fn should_convert_symbol() {
        // Given
        let symbol = Symbol { base: Asset::ETH, quote: Asset::BTC };

        // When
        let resp = symbol_to_string(&symbol);

        // Then
        assert_eq!("ethbtc", resp)
    }
}
