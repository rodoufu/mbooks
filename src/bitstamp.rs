use crate::types::{
    Level,
    Symbol,
    Summary,
    WebsocketError,
};
use futures_util::{
    SinkExt,
    StreamExt,
};
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
};
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::Message,
};

#[derive(Debug, Deserialize)]
struct Data {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

impl TryInto<Summary> for Data {
    type Error = WebsocketError;

    fn try_into(self) -> Result<Summary, Self::Error> {
        let mut bids = Vec::with_capacity(self.bids.len());
        for bid in &self.bids {
            bids.push(Level {
                exchange: "bitstamp".to_string(),
                price: bid[0].parse::<f64>().map_err(WebsocketError::ParseError)?,
                quantity: bid[1].parse::<f64>().map_err(WebsocketError::ParseError)?,
            });
        }

        let mut asks = Vec::with_capacity(self.asks.len());
        for ask in &self.asks {
            asks.push(Level {
                exchange: "bitstamp".to_string(),
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

#[derive(Debug, Deserialize)]
#[serde(tag = "event")]
enum WebSocketEvent {
    #[serde(rename(deserialize = "bts:subscription_succeeded"))]
    Succeeded,
    #[serde(rename(deserialize = "data"))]
    Data { data: Data },
}

fn symbol_to_string(symbol: &Symbol) -> String {
    format!("{}{}", symbol.base.to_string(), symbol.quote.to_string()).to_lowercase()
}

pub async fn run_bitstamp(
    log: Logger,
    summary_tx: UnboundedSender<Summary>,
    symbol: &Symbol, depth: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let tracer = global::tracer("run_bitstamp");
    let span = tracer.start("running bitstamp");
    let cx = Context::current_with_span(span);
    let log = log.new(o!("exchange" => "bitstamp", "symbol" => format!("{:?}", symbol)));
    info!(log, "running bitstamp");

    let connect_addr = "wss://ws.bitstamp.net";

    let url = url::Url::parse(connect_addr)?;
    info!(log, "bitstamp url"; "url" => format!("{:?}", url));

    let (ws_stream, _) = connect_async(url)
        .with_context(cx.clone())
        .await.expect("Failed to connect");
    info!(log, "WebSocket handshake has been successfully completed");

    let (mut write, read) = ws_stream.split();
    write.send(Message::Text(
        format!(
            "{{\"event\":\"bts:subscribe\",\"data\":{{\"channel\": \"order_book_{}\"}}}}",
            symbol_to_string(symbol),
        ))
    ).with_context(cx.clone()).await?;

    read.for_each(|message| async {
        debug!(log, "websocket got message");
        let message_data = message.unwrap().into_data();
        let bitstamp_parse: serde_json::Result<WebSocketEvent> = serde_json::from_slice(
            &message_data,
        );

        match bitstamp_parse {
            Ok(event) => {
                match event {
                    WebSocketEvent::Succeeded => {}
                    WebSocketEvent::Data { mut data } => {
                        // Keeping only the updates within the depth
                        if data.bids.len() > depth as usize {
                            data.bids = data.bids.as_slice()[..(depth as usize)].to_vec();
                        }
                        if data.asks.len() > depth as usize {
                            data.asks = data.asks.as_slice()[..(depth as usize)].to_vec();
                        }

                        match TryInto::<Summary>::try_into(data) {
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

    Ok(())
}

mod test {
    use crate::{
        bitstamp::{
            symbol_to_string,
            WebSocketEvent,
        },
        types::{
            Asset,
            Symbol,
        },
    };

    #[test]
    fn should_parse_a_subscribe() {
        // Given
        let msg = r#"{"event":"bts:subscription_succeeded","channel":"order_book_ethbtc","data":{}}"#;

        // When
        let resp: WebSocketEvent = serde_json::from_str(msg).unwrap();

        // Then
        if let WebSocketEvent::Succeeded = resp {
            assert!(true);
        } else {
            assert!(false, "not a subscribe");
        }
    }

    #[test]
    fn should_parse_data() {
        // Given
        let msg = r#"{"data":{"timestamp":"1666200249","microtimestamp":"1666200249249913","bids":[["0.06760079","0.55000000"],["0.06759456","5.79242377"]],"asks":[["0.06764067","0.55000000"],["0.06764614","5.78800796"],["0.06765134","7.71643786"]]},"channel":"order_book_ethbtc","event":"data"}"#;

        // When
        let resp: WebSocketEvent = serde_json::from_str(msg).unwrap();

        // Then
        if let WebSocketEvent::Data { data } = resp {
            assert_eq!(2, data.bids.len());
            assert_eq!(3, data.asks.len());
        } else {
            assert!(false, "not a subscribe");
        }
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