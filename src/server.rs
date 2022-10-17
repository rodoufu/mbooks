use crate::orderbook::{
    Empty,
    Summary,
    orderbook_aggregator_server::{
        OrderbookAggregator,
        OrderbookAggregatorServer,
    },
};
use tonic::{
    transport::Server,
    Response,
    Status,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Default)]
pub struct OrderbookAggregatorImpl {}

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorImpl {
    type BookSummaryStream = ReceiverStream<Result<Summary, Status>>;

    async fn book_summary(
        &self, _: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::BookSummaryStream>, tonic::Status> {
        let (mut tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            // let mut summary = Summary::new();
            for i in 0..1000 {
                tx.send(Ok(Summary {
                    spread: i as f64,
                    bids: Vec::new(),
                    asks: Vec::new(),
                })).await.unwrap();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub async fn run_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("[::1]:{}", port).parse().unwrap();
    let orderbook = OrderbookAggregatorImpl::default();

    println!("Orderbook server listening on {}", addr);

    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook))
        .serve(addr)
        .await?;

    Ok(())
}
