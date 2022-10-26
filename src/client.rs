use crate::orderbook::{
    Empty,
    orderbook_aggregator_client::OrderbookAggregatorClient,
};
use opentelemetry::{
    Key,
    global,
    trace::{
        FutureExt,
        TraceContextExt,
        Tracer,
    },
    Context,
};
use slog::{
    Logger,
    info,
};
use tonic::Request;
use tokio::sync::broadcast::Receiver;

/// Connects to the server and listen to all received updates printing in the log.
pub async fn run_client(
    log: Logger,
    shutdown_receiver: &mut Receiver<String>,
    address: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let tracer = global::tracer("run_client");
    let span = tracer.start(format!("running client at: {}", address));
    let cx = Context::current_with_span(span);

    info!(log, "starting client"; "address" => &address);
    let mut client = OrderbookAggregatorClient::connect(
        address,
    ).with_context(cx.clone()).await?;

    info!(log, "requesting book_summary");
    let response = client.book_summary(Request::new(Empty {})).with_context(cx.clone()).await?;
    let mut inbound = response.into_inner();

    loop {
        tokio::select! {
            message = inbound.message().with_context(cx.clone()) => {
                if let Some(summary) = message? {
                    cx.span().add_event("got summary", vec![Key::new("spread").f64(summary.spread)]);
                    info!(log, "got a summary"; "summary" => format!("{:?}", summary));
                } else {
                    info!(log, "no more messages");
                    return Ok(());
                }
            }
            _ = shutdown_receiver.recv() => {
                info!(log, "application is shutting down, closing client");
                return Ok(());
            }
        }
    }
}