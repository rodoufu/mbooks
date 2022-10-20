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
    Logger,
    info,
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
            bids: self.bids.iter().take(self.depth).map(|x| x.clone()).collect(),
            asks: self.asks.iter().take(self.depth).map(|x| x.clone()).collect(),
        }
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let tracer = global::tracer("OrderbookMerger");
        let span = tracer.start("OrderbookMerger");
        let cx = Context::current_with_span(span);
        info!(self.log, "starting merger");

        while let Some(summary) = self.summary_receiver.recv().with_context(cx.clone()).await {
            // TODO maybe avoid copy
            (self.bids, self.asks) = OrderbookMerger::process_summary(
                self.log.clone(), self.bids.clone(), self.asks.clone(), summary,
            );
            let summary_update = self.summary();
            self.summary_sender.send(summary_update.into()).unwrap();
        }

        Ok(())
    }

    fn process_summary(
        log: Logger, bids: Vec<Level>, asks: Vec<Level>, summary: types::Summary,
    ) -> (Vec<Level>, Vec<Level>) {
        let mut bids = bids;
        let mut asks = asks;

        if summary.asks.is_empty() && summary.bids.is_empty() {
            return (bids, asks);
        }
        let mut exchange = "".to_string();
        let _: Vec<_> = summary.asks.iter().take(1)
            .map(|x| exchange = x.exchange.clone()).collect();
        let _: Vec<_> = summary.bids.iter().take(1)
            .map(|x| exchange = x.exchange.clone()).collect();
        info!(
            log, "processing summary";
            "exchange" => &exchange, "bids" => bids.len(), "asks" => asks.len()
        );

        // Removing the old entries for the exchange we are receiving
        bids.retain(|x| x.exchange != exchange);
        asks.retain(|x| x.exchange != exchange);

        (Self::process_summary_bids(&summary, bids), Self::process_summary_asks(&summary, asks))
    }

    fn process_summary_asks(summary: &types::Summary, asks: Vec<Level>) -> Vec<Level> {
        let mut resp_asks = Vec::new();
        let mut idx_asks = 0;
        let mut idx_s_asks = 0;

        while idx_asks < asks.len() && idx_s_asks < summary.asks.len() {
            if asks[idx_asks].price < summary.asks[idx_s_asks].price {
                // TODO maybe I can save the clone
                resp_asks.push(asks[idx_asks].clone());
                idx_asks += 1;
            } else {
                resp_asks.push(summary.asks[idx_s_asks].clone());
                idx_s_asks += 1;
            }
        }

        while idx_asks < asks.len() {
            // TODO maybe I can save the clone
            resp_asks.push(asks[idx_asks].clone());
            idx_asks += 1;
        }

        while idx_s_asks < summary.asks.len() {
            // TODO maybe I can save the clone
            resp_asks.push(summary.asks[idx_s_asks].clone());
            idx_s_asks += 1;
        }

        resp_asks
    }

    fn process_summary_bids(summary: &types::Summary, bids: Vec<Level>) -> Vec<Level> {
        let mut resp_bids = Vec::new();
        let mut idx_bids = 0;
        let mut idx_s_bids = 0;

        while idx_bids < bids.len() && idx_s_bids < summary.bids.len() {
            if bids[idx_bids].price > summary.bids[idx_s_bids].price {
                // TODO maybe I can save the clone
                resp_bids.push(bids[idx_bids].clone());
                idx_bids += 1;
            } else {
                resp_bids.push(summary.bids[idx_s_bids].clone());
                idx_s_bids += 1;
            }
        }

        while idx_bids < bids.len() {
            // TODO maybe I can save the clone
            resp_bids.push(bids[idx_bids].clone());
            idx_bids += 1;
        }

        while idx_s_bids < summary.bids.len() {
            // TODO maybe I can save the clone
            resp_bids.push(summary.bids[idx_s_bids].clone());
            idx_s_bids += 1;
        }

        resp_bids
    }
}

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

        test_sender.send(Summary {
            bids: vec![
                Level {
                    exchange: "binance".to_string(),
                    price: 1.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: "binance".to_string(),
                    price: 0.9,
                    quantity: 10.0,
                },
            ],
            asks: vec![
                Level {
                    exchange: "binance".to_string(),
                    price: 2.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: "binance".to_string(),
                    price: 3.0,
                    quantity: 10.0,
                },
                Level {
                    exchange: "binance".to_string(),
                    price: 4.0,
                    quantity: 10.0,
                },
            ],
        }).unwrap();
        drop(test_sender);
        merger.start().await.unwrap();

        assert_eq!(3, merger.asks.len());
        assert_eq!(2, merger.bids.len());
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
                    price: 1.1,
                    quantity: 10.0,
                },
                Level {
                    exchange: bitstamp.clone(),
                    price: 1.05,
                    quantity: 10.0,
                },
            ],
            asks: vec![
                Level {
                    exchange: bitstamp.clone(),
                    price: 2.1,
                    quantity: 10.0,
                },
                Level {
                    exchange: bitstamp.clone(),
                    price: 3.1,
                    quantity: 10.0,
                },
            ],
        }).unwrap();
        drop(test_sender);
        merger.start().await.unwrap();

        assert_eq!(4, merger.asks.len());
        assert_eq!(4, merger.bids.len());
    }
}
