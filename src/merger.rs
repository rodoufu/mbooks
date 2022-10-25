use crate::{
    orderbook,
    types::{
        self,
        Level,
    },
};
use opentelemetry::{
    Context,
    global,
    trace::{
        FutureExt,
        TraceContextExt,
        Tracer,
    },
};
use slog::{
    error,
    info,
    Logger,
};
use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

pub struct OrderbookMerger {
    log: Logger,
    summary_receiver: UnboundedReceiver<types::Summary>,
    summary_sender: UnboundedSender<orderbook::Summary>,
    bids: Vec<Level>,
    asks: Vec<Level>,
    depth: usize,
}

impl OrderbookMerger {
    pub fn new(
        log: Logger,
        summary_receiver: UnboundedReceiver<types::Summary>,
        summary_sender: UnboundedSender<orderbook::Summary>,
        depth: usize,
    ) -> Self {
        Self {
            log,
            summary_receiver,
            summary_sender,
            depth,
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    fn summary(&self) -> types::Summary {
        types::Summary {
            bids: self.bids.iter().take(self.depth).cloned().collect(),
            asks: self.asks.iter().take(self.depth).cloned().collect(),
        }
    }

    pub async fn start(
        &mut self,
        shutdown_sender: tokio::sync::broadcast::Sender<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tracer = global::tracer("OrderbookMerger");
        let span = tracer.start("OrderbookMerger");
        let cx = Context::current_with_span(span);
        info!(self.log, "starting merger");

        let mut shutdown_receiver = shutdown_sender.subscribe();
        loop {
            tokio::select! {
                message = self.summary_receiver.recv().with_context(cx.clone()) => {
                    if let Some (summary) = message {
                        // Avoiding having to clone bids and asks from self
                        let mut asks = Vec::new();
                        std::mem::swap(&mut asks, &mut self.asks);
                        let mut bids = Vec::new();
                        std::mem::swap(&mut bids, &mut self.bids);

                        (self.bids, self.asks) = Self::process_summary(
                            self.log.clone(), bids, asks, summary,
                        );

                        if let Err(err) = self.summary_sender.send(self.summary().into()) {
                            error!(self.log, "problem sending summary"; "error" => format!("{}", err));
                        }
                    } else {
                        info!(self.log, "no more messages at Merger::start");
                        return Ok(());
                    }
                }
                _ = shutdown_receiver.recv() => {
                    info!(self.log, "application is shutting down, closing merger");
                    return Ok(());
                }
            }
        }
    }

    fn process_summary(
        log: Logger, bids: Vec<Level>, asks: Vec<Level>, summary: types::Summary,
    ) -> (Vec<Level>, Vec<Level>) {
        if summary.asks.is_empty() && summary.bids.is_empty() {
            return (bids, asks);
        }

        let mut summary = summary;
        // Avoiding having to clone bids and asks from self
        let mut summary_asks = Vec::new();
        std::mem::swap(&mut summary_asks, &mut summary.asks);
        let mut summary_bids = Vec::new();
        std::mem::swap(&mut summary_bids, &mut summary.bids);

        let (bids, exchange_bids) = Self::process_summary_asks_bids(
            summary_bids, bids, -1.0,
        );
        let (asks, exchange_aks) = Self::process_summary_asks_bids(
            summary_asks, asks, 1.0,
        );

        info!(
            log, "processing summary";
            "exchange" => exchange_bids.unwrap_or_else(|| exchange_aks.unwrap()),
            "bids" => bids.len(), "asks" => asks.len()
        );

        (bids, asks)
    }

    fn process_summary_asks_bids(
        summary_asks_bids: Vec<Level>, asks_bids: Vec<Level>, multiplier: f64,
    ) -> (Vec<Level>, Option<String>) {
        if summary_asks_bids.is_empty() {
            return (asks_bids, None);
        }

        let mut idx_asks_bids = 0;
        let mut idx_summary = 0;
        let mut idx_resp = 0;

        let mut asks_bids = asks_bids;
        let mut summary_asks_bids = summary_asks_bids;

        let exchange = summary_asks_bids[0].exchange.clone();
        let count_exchange = asks_bids.iter()
            .filter(|x| x.exchange == exchange).count();

        let mut resp = Vec::with_capacity(
            asks_bids.len() - count_exchange + summary_asks_bids.len(),
        );
        for _ in 0..resp.capacity() {
            resp.push(Level {
                exchange: "".to_string(),
                price: 0.0,
                quantity: 0.0,
            });
        }

        while idx_asks_bids < asks_bids.len() && idx_summary < summary_asks_bids.len() {
            // Ignoring outdated information already in the orderbook for this exchange
            if asks_bids[idx_asks_bids].exchange == exchange {
                idx_asks_bids += 1;
                continue;
            }

            if asks_bids[idx_asks_bids].price * multiplier < summary_asks_bids[idx_summary].price * multiplier {
                std::mem::swap(&mut resp[idx_resp], &mut asks_bids[idx_asks_bids]);
                idx_asks_bids += 1;
                idx_resp += 1;
            } else {
                std::mem::swap(&mut resp[idx_resp], &mut summary_asks_bids[idx_summary]);
                idx_summary += 1;
                idx_resp += 1;
            }
        }

        while idx_asks_bids < asks_bids.len() {
            // Ignoring outdated information already in the orderbook for this exchange
            if asks_bids[idx_asks_bids].exchange == exchange {
                idx_asks_bids += 1;
                continue;
            }
            std::mem::swap(&mut resp[idx_resp], &mut asks_bids[idx_asks_bids]);
            idx_asks_bids += 1;
            idx_resp += 1;
        }

        while idx_summary < summary_asks_bids.len() {
            std::mem::swap(&mut resp[idx_resp], &mut summary_asks_bids[idx_summary]);
            idx_summary += 1;
            idx_resp += 1;
        }

        (resp, Some(exchange))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        merger::OrderbookMerger,
        types::{
            Level,
            Summary,
        },
    };
    use slog::{
        Logger,
        Drain,
        o,
    };
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn should_add_to_an_empty_orderbook() {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let logger = Logger::root(
            slog_term::FullFormat::new(plain)
                .build().fuse(), o!(),
        );
        let (summary_sender, _summary_receiver) = mpsc::unbounded_channel();
        let (test_sender, summary_receiver) = mpsc::unbounded_channel();
        let mut merger = OrderbookMerger::new(
            logger, summary_receiver, summary_sender, 2,
        );

        let binance = "binance".to_string();
        test_sender.send(Summary {
            bids: vec![
                Level {
                    exchange: binance.clone(),
                    price: 1.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 0.9,
                    quantity: 10.0,
                },
            ],
            asks: vec![
                Level {
                    exchange: binance.clone(),
                    price: 2.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 3.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 4.0,
                    quantity: 10.0,
                },
            ],
        }).unwrap();
        drop(test_sender);
        merger.start().await.unwrap();

        assert_eq!(3, merger.asks.len());
        assert_eq!(
            merger.asks,
            vec![
                Level {
                    exchange: binance.clone(),
                    price: 2.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 3.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 4.0,
                    quantity: 10.0,
                },
            ],
        );

        assert_eq!(2, merger.bids.len());
        assert_eq!(
            merger.bids,
            vec![
                Level {
                    exchange: binance.clone(),
                    price: 1.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 0.9,
                    quantity: 10.0,
                },
            ],
        );
        assert_eq!(1.0, merger.summary().spread());
    }

    #[tokio::test]
    async fn should_add_to_an_existing_orderbook() {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let logger = Logger::root(
            slog_term::FullFormat::new(plain)
                .build().fuse(), o!(),
        );
        let (summary_sender, _summary_receiver) = mpsc::unbounded_channel();
        let (test_sender, summary_receiver) = mpsc::unbounded_channel();
        let mut merger = OrderbookMerger::new(
            logger, summary_receiver, summary_sender, 2,
        );

        let binance = "binance".to_string();
        let bitstamp = "bitstamp".to_string();
        merger.bids = vec![
            Level {
                exchange: binance.clone(),
                price: 1.0,
                quantity: 10.0,
            },
            Level {
                exchange: bitstamp.clone(),
                price: 0.9,
                quantity: 10.0,
            },
        ];
        merger.asks = vec![
            Level {
                exchange: binance.clone(),
                price: 2.0,
                quantity: 10.0,
            },
            Level {
                exchange: bitstamp.clone(),
                price: 3.0,
                quantity: 10.0,
            },
        ];

        test_sender.send(Summary {
            bids: vec![
                Level {
                    exchange: binance.clone(),
                    price: 1.1,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 1.05,
                    quantity: 10.0,
                },
            ],
            asks: vec![
                Level {
                    exchange: binance.clone(),
                    price: 2.1,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 3.1,
                    quantity: 10.0,
                },
            ],
        }).unwrap();

        test_sender.send(Summary {
            bids: vec![
                Level {
                    exchange: bitstamp.clone(),
                    price: 1.11,
                    quantity: 10.0,
                },
                Level {
                    exchange: bitstamp.clone(),
                    price: 1.051,
                    quantity: 10.0,
                },
            ],
            asks: vec![
                Level {
                    exchange: bitstamp.clone(),
                    price: 2.11,
                    quantity: 10.0,
                },
                Level {
                    exchange: bitstamp.clone(),
                    price: 3.11,
                    quantity: 10.0,
                },
            ],
        }).unwrap();
        drop(test_sender);
        merger.start().await.unwrap();

        assert_eq!(4, merger.bids.len());
        assert_eq!(
            merger.bids,
            vec![
                Level {
                    exchange: bitstamp.clone(),
                    price: 1.11,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 1.1,
                    quantity: 10.0,
                },
                Level {
                    exchange: bitstamp.clone(),
                    price: 1.051,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 1.05,
                    quantity: 10.0,
                },
            ],
        );

        assert_eq!(4, merger.asks.len());
        assert_eq!(
            merger.asks,
            vec![
                Level {
                    exchange: binance.clone(),
                    price: 2.1,
                    quantity: 10.0,
                },
                Level {
                    exchange: bitstamp.clone(),
                    price: 2.11,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 3.1,
                    quantity: 10.0,
                },
                Level {
                    exchange: bitstamp.clone(),
                    price: 3.11,
                    quantity: 10.0,
                },
            ],
        );

        let summary = merger.summary();
        assert_eq!(0.99, summary.spread());
        assert_eq!(2, summary.bids.len());
        assert_eq!(
            summary.bids,
            vec![
                Level {
                    exchange: bitstamp.clone(),
                    price: 1.11,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 1.1,
                    quantity: 10.0,
                },
            ],
        );

        assert_eq!(2, summary.asks.len());
        assert_eq!(
            summary.asks,
            vec![
                Level {
                    exchange: binance.clone(),
                    price: 2.1,
                    quantity: 10.0,
                },
                Level {
                    exchange: bitstamp.clone(),
                    price: 2.11,
                    quantity: 10.0,
                },
            ],
        );
    }

    #[tokio::test]
    async fn should_add_empty_summary_to_an_existing_orderbook() {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let logger = Logger::root(
            slog_term::FullFormat::new(plain)
                .build().fuse(), o!(),
        );
        let (summary_sender, _summary_receiver) = mpsc::unbounded_channel();
        let (test_sender, summary_receiver) = mpsc::unbounded_channel();
        let mut merger = OrderbookMerger::new(
            logger, summary_receiver, summary_sender, 2,
        );

        let binance = "binance".to_string();
        let bitstamp = "bitstamp".to_string();
        merger.bids = vec![
            Level {
                exchange: binance.clone(),
                price: 1.0,
                quantity: 10.0,
            },
            Level {
                exchange: bitstamp.clone(),
                price: 0.9,
                quantity: 10.0,
            },
        ];
        merger.asks = vec![
            Level {
                exchange: binance.clone(),
                price: 2.0,
                quantity: 10.0,
            },
            Level {
                exchange: bitstamp.clone(),
                price: 3.0,
                quantity: 10.0,
            },
        ];

        test_sender.send(Summary {
            bids: Vec::new(),
            asks: Vec::new(),
        }).unwrap();

        drop(test_sender);
        merger.start().await.unwrap();

        assert_eq!(2, merger.asks.len());
        assert_eq!(2, merger.bids.len());
    }

    #[tokio::test]
    async fn should_replace_outdated_data_from_same_exchange() {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let logger = Logger::root(
            slog_term::FullFormat::new(plain)
                .build().fuse(), o!(),
        );
        let (summary_sender, _summary_receiver) = mpsc::unbounded_channel();
        let (test_sender, summary_receiver) = mpsc::unbounded_channel();
        let mut merger = OrderbookMerger::new(
            logger, summary_receiver, summary_sender, 2,
        );

        let binance = "binance".to_string();
        let bitstamp = "bitstamp".to_string();
        merger.bids = vec![
            Level {
                exchange: binance.clone(),
                price: 1.0,
                quantity: 10.0,
            },
            Level {
                exchange: bitstamp.clone(),
                price: 0.9,
                quantity: 10.0,
            },
        ];
        merger.asks = vec![
            Level {
                exchange: binance.clone(),
                price: 2.0,
                quantity: 10.0,
            },
            Level {
                exchange: bitstamp.clone(),
                price: 3.0,
                quantity: 10.0,
            },
        ];

        test_sender.send(Summary {
            bids: vec![
                Level {
                    exchange: binance.clone(),
                    price: 0.8,
                    quantity: 10.0,
                },
            ],
            asks: vec![
                Level {
                    exchange: binance.clone(),
                    price: 4.0,
                    quantity: 10.0,
                },
            ],
        }).unwrap();

        drop(test_sender);
        merger.start().await.unwrap();

        assert_eq!(2, merger.bids.len());
        assert_eq!(
            merger.bids,
            vec![
                Level {
                    exchange: bitstamp.clone(),
                    price: 0.9,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 0.8,
                    quantity: 10.0,
                },
            ],
        );
        assert_eq!(2, merger.asks.len());
        assert_eq!(
            merger.asks,
            vec![
                Level {
                    exchange: bitstamp.clone(),
                    price: 3.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: binance.clone(),
                    price: 4.0,
                    quantity: 10.0,
                },
            ],
        );
    }
}
