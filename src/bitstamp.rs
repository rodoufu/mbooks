use url;
use futures_channel;
use futures_util::{future, pin_mut, SinkExt, StreamExt};
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
use crate::types::{Level, Symbol, Summary, WebsocketError};

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
    summary_tx: UnboundedSender<Summary>,
    symbol: &Symbol, depth: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let tracer = global::tracer("run_bitstamp");
    let span = tracer.start("running bitstamp");
    let cx = Context::current_with_span(span);

    let connect_addr = "wss://ws.bitstamp.net";

    let url = url::Url::parse(&connect_addr)?;

    // let (stdin_tx, stdin_rx) = futures_channel::mpsc::unbounded();
    // tokio::spawn(read_stdin(stdin_tx));

    let (ws_stream, _) = connect_async(url)
        .with_context(cx.clone())
        .await.expect("Failed to connect");
    println!("Bitstamp WebSocket handshake has been successfully completed to {:?}", symbol);

    let (mut write, read) = ws_stream.split();
    write.send(Message::Text(
        format!(
            "{{\"event\":\"bts:subscribe\",\"data\":{{\"channel\": \"order_book_{}\"}}}}",
            symbol_to_string(symbol),
        ))
    ).with_context(cx.clone()).await?;

    // let stdin_to_ws = stdin_rx.map(Ok).forward(write);
    read.for_each(|message| async {
        let message_data = message.unwrap().into_data();
        let mut bitstamp_parse: serde_json::Result<WebSocketEvent> = serde_json::from_slice(
            &message_data,
        );

        match bitstamp_parse {
            Ok(event) => {
                match event {
                    WebSocketEvent::Succeeded => {}
                    WebSocketEvent::Data {data} => {
                        // depth_update.asks.resize(depth);
                        // depth_update.bids.resize(depth);
                        match data.try_into() {
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

    // pin_mut!(stdin_to_ws, ws_to_stdout)
    // future::select(stdin_to_ws, ws_to_stdout).await;
    Ok(())
}

mod test {
    use crate::bitstamp::{symbol_to_string, WebSocketEvent};
    use crate::types::{Asset, Symbol};

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