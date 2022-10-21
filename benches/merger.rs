use criterion::*;
use criterion::async_executor::FuturesExecutor;
use mbooks::{
    merger::OrderbookMerger,
    types::{
        Level,
        Summary,
    },
};
use slog::o;
use tokio::sync::mpsc;

fn merger_benchmark(c: &mut Criterion) {
    for size in vec![2, 5, 10, 20, 50, 100, 200, 500] {
        let binance = "binance".to_string();
        let mut bids = Vec::with_capacity(size);
        let mut asks = Vec::with_capacity(size);
        for i in 0..size {
            bids.push(Level {
                exchange: binance.clone(),
                price: 1.0 + (size - i) as f64,
                quantity: 10.0,
            });
            asks.push(Level {
                exchange: binance.clone(),
                price: (2 * size + i) as f64,
                quantity: 10.0,
            });
        }
        let summary_binance = Summary {
            asks,
            bids,
        };

        let bitstamp = "bitstamp".to_string();
        let mut bids = Vec::with_capacity(size);
        let mut asks = Vec::with_capacity(size);
        for i in 0..size {
            bids.push(Level {
                exchange: bitstamp.clone(),
                price: 2.0 + (size - i) as f64,
                quantity: 10.0,
            });
            asks.push(Level {
                exchange: bitstamp.clone(),
                price: (3 * size + i) as f64,
                quantity: 10.0,
            });
        }
        let summary_bitstamp = Summary {
            asks,
            bids,
        };

        c.bench_function(format!("merger merging {} objects", size).as_str(), move |b| {
            b.to_async(FuturesExecutor).iter(|| async {
                let drain = slog::Discard;
                let logger = slog::Logger::root(drain, o!());

                let (summary_sender, _summary_receiver) = mpsc::unbounded_channel();
                let (test_sender, summary_receiver) = mpsc::unbounded_channel();
                let mut merger = OrderbookMerger::new(
                    logger.clone(), summary_receiver, summary_sender, 2,
                );

                test_sender.send(summary_binance.clone()).unwrap();
                test_sender.send(summary_bitstamp.clone()).unwrap();

                test_sender.send(summary_binance.clone()).unwrap();
                test_sender.send(summary_bitstamp.clone()).unwrap();
                drop(test_sender);
                merger.start().await.unwrap();
            })
        });
    }
}

criterion_group!(benches, merger_benchmark);
criterion_main!(benches);
