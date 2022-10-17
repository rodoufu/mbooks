use crate::orderbook::{
    Empty,
    Summary,
    orderbook_aggregator_client::{
        OrderbookAggregatorClient,
    },
};
use tonic::{
    transport::Server,
    Request,
    Response,
    Status,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://[::1]:50051").await?;
    let response = client.book_summary(Request::new(Empty {})).await?;
    let mut inbound = response.into_inner();

    while let Some(summary) = inbound.message().await? {
        println!("Summary: {:?}", summary);
    }

    Ok(())
}