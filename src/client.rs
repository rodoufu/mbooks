use crate::orderbook::{
    Empty,
    orderbook_aggregator_client::{
        OrderbookAggregatorClient,
    },
};
use opentelemetry::{
    Key,
    global,
    trace::{FutureExt, TraceContextExt, Tracer},
    Context,
};
use tonic::{
    Request,
};

pub async fn run_client(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let tracer = global::tracer("run_client");
    let span = tracer.start(format!("running client at: {}", port));
    let cx = Context::current_with_span(span);

    let mut client = OrderbookAggregatorClient::connect(
        format!("http://[::1]:{}", port),
    ).with_context(cx.clone()).await?;
    let response = client.book_summary(Request::new(Empty {})).with_context(cx.clone()).await?;
    let mut inbound = response.into_inner();

    while let Some(summary) = inbound.message().with_context(cx.clone()).await? {
        cx.span().add_event("got summary", vec![Key::new("spread").f64(summary.spread)]);
        println!("Summary: {:?}", summary);
    }

    Ok(())
}