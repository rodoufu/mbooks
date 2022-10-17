use crate::orderbook::{
    Empty,
    orderbook_aggregator_client::{
        OrderbookAggregatorClient,
    },
};
use tonic::{
    Request,
};

pub async fn run_client(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect(
        format!("http://[::1]:{}", port),
    ).await?;
    let response = client.book_summary(Request::new(Empty {})).await?;
    let mut inbound = response.into_inner();

    while let Some(summary) = inbound.message().await? {
        println!("Summary: {:?}", summary);
    }

    Ok(())
}