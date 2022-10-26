use crate::{
    binance::run_binance,
    bitstamp::run_bitstamp,
    orderbook::{
        Empty,
        Summary,
        orderbook_aggregator_server::{
            OrderbookAggregator,
            OrderbookAggregatorServer,
        },
    },
    merger::OrderbookMerger,
    types::Symbol,
};
use opentelemetry::{
    global,
    trace::{
        FutureExt,
        TraceContextExt,
        Tracer,
    },
    Context,
};
use slog::{
    error,
    Logger,
    info,
};
use tonic::{
    transport::Server,
    Response,
    Status,
};
use tokio::sync::{
    mpsc::{
        self,
        Receiver,
        Sender,
        UnboundedReceiver,
    },
    Mutex,
};
use tokio_stream::wrappers::ReceiverStream;

pub type ClientSubscription = Sender<Result<Summary, Status>>;

/// OrderbookAggregatorImpl the gRPC server implementation.
pub struct OrderbookAggregatorImpl {
    log: Logger,
    clients_to_connect_sender: Sender<ClientSubscription>,
}

impl OrderbookAggregatorImpl {
    fn new(
        log: Logger,
        clients_to_connect_sender: Sender<ClientSubscription>,
    ) -> Self {
        Self {
            log,
            clients_to_connect_sender,
        }
    }

    /// Listens to clients trying to connect and add them to the list of targets who will receive
    /// the summary updates.
    async fn listen_clients_to_connect(
        log: Logger,
        shutdown_receiver: tokio::sync::broadcast::Receiver<String>,
        targets: &Mutex<Vec<ClientSubscription>>,
        clients_to_connect_receiver: Receiver<ClientSubscription>,
    ) -> Result<(), tonic::transport::Error> {
        let mut shutdown_receiver = shutdown_receiver;
        let mut clients_to_connect_receiver = clients_to_connect_receiver;
        loop {
            tokio::select! {
                message = clients_to_connect_receiver.recv() => {
                    if let Some(client_to_connect) = message {
                        targets.lock().await.push(client_to_connect);
                    } else {
                        info!(log, "no more messages listen_clients_to_connect");
                        return Ok(());
                    }
                }
                _ = shutdown_receiver.recv() => {
                    info!(log, "application is shutting down, closing listen_clients_to_connect");
                    return Ok(());
                }
            }
        }
    }

    /// Listens to the summary updates from the WebSocket connections and updates internal book.
    async fn listen_summaries(
        log: Logger,
        shutdown_receiver: tokio::sync::broadcast::Receiver<String>,
        targets: &Mutex<Vec<ClientSubscription>>,
        grpc_receiver: UnboundedReceiver<Summary>,
    ) -> Result<(), tonic::transport::Error> {
        let mut shutdown_receiver = shutdown_receiver;
        let mut grpc_receiver = grpc_receiver;
        loop {
            tokio::select! {
                message = grpc_receiver.recv() => {
                    if let Some(summary) = message {
                        let mut it_targets = targets.lock().await;
                        let mut resp = Vec::new();
                        for target in it_targets.iter() {
                            if let Err(err) = target.send(Ok(summary.clone())).await {
                                info!(log, "client dropped"; "error" => format!("{:?}", err));
                            } else {
                                resp.push(target.clone());
                            }
                        }

                        *it_targets = resp;
                    } else {
                        info!(log, "no more messages listen_summaries");
                        return Ok(());
                    }
                }
                _ = shutdown_receiver.recv() => {
                    info!(log, "application is shutting down, closing listen_summaries");
                    return Ok(());
                }
            }
        }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorImpl {
    type BookSummaryStream = ReceiverStream<Result<Summary, Status>>;

    async fn book_summary(
        &self, _: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::BookSummaryStream>, tonic::Status> {
        info!(self.log, "got a new client");
        let (tx, rx) = mpsc::channel(4);

        if let Err(err) = self.clients_to_connect_sender.send(tx).await {
            error!(self.log, "error adding client"; "error" => format!("{:?}", err));
            Err(Status::internal("unable to add client"))
        } else {
            info!(self.log, "client added successfully");
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }
}

/// Waits for the shutdown signal which will come from the channel.
/// It is used to gracefully stop the `hyper` server answering to the gRPC requests.
async fn shutdown_signal(log: Logger, shutdown_receiver: tokio::sync::broadcast::Receiver<String>) {
    info!(log, "waiting for the server to get a shutdown signal");
    let mut shutdown_receiver = shutdown_receiver;
    let _ = shutdown_receiver.recv().await;
    info!(log, "got the shutdown signal, closing grpc server");
}

/// Creates and runs the gRPC server.
async fn run_grpc_server(
    log: Logger,
    shutdown_sender: tokio::sync::broadcast::Sender<String>,
    grpc_receiver: UnboundedReceiver<Summary>,
    address: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let tracer = global::tracer("run_server");
    let span = tracer.start(format!("running server at: {}", &address));
    let cx = Context::current_with_span(span);
    info!(log, "starting server"; "address" => &address);

    let addr = address.parse()
        .map_err(|e| format!("problem parsing address: {}", e))?;

    let targets = Mutex::new(Vec::new());
    let (clients_to_connect_sender, clients_to_connect_receiver) = mpsc::channel(10);
    let orderbook = OrderbookAggregatorImpl::new(
        log.clone(),
        clients_to_connect_sender,
    );

    info!(log, "Orderbook server listening"; "address" => addr);

    let grpc_server_shutdown_receiver = shutdown_sender.subscribe();

    let run_grpc_server = Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook))
        .serve_with_shutdown(addr, shutdown_signal(log.clone(), grpc_server_shutdown_receiver))
        .with_context(cx);

    let listen_summaries_shutdown_receiver = shutdown_sender.subscribe();
    let listen_clients_to_connect_shutdown_receiver = shutdown_sender.subscribe();
    drop(shutdown_sender);
    tokio::try_join!(
        OrderbookAggregatorImpl::listen_summaries(
            log.clone(),
            listen_summaries_shutdown_receiver,
            &targets,
            grpc_receiver,
        ),
        OrderbookAggregatorImpl::listen_clients_to_connect(
            log.clone(),
            listen_clients_to_connect_shutdown_receiver,
            &targets,
            clients_to_connect_receiver,
        ),
        run_grpc_server,
    )?;

    info!(log, "run_grpc_server has ended");

    Ok(())
}

/// Starts the `OrderbookMerger`, the exchange connections, the gRPC server and tries to join all
/// those futures.
pub async fn run_server(
    log: Logger,
    shutdown_sender: tokio::sync::broadcast::Sender<String>,
    address: String, pair: Symbol, depth: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (summary_sender, summary_receiver) = mpsc::unbounded_channel();
    let (grpc_sender, grpc_receiver) = mpsc::unbounded_channel();

    let mut merger = OrderbookMerger::new(
        log.clone(), summary_receiver, grpc_sender, depth,
    );

    let binance_receiver = shutdown_sender.subscribe();
    let bitstamp_receiver = shutdown_sender.subscribe();
    let grpc_shutdown_sender = shutdown_sender.clone();
    let merger_shutdown_sender = shutdown_sender;
    match tokio::try_join!(
        run_binance(
            log.clone(), binance_receiver,
            summary_sender.clone(), &pair, depth,
        ),
        run_bitstamp(
            log.clone(), bitstamp_receiver,
            summary_sender, &pair, depth,
        ),
        run_grpc_server(log.clone(), grpc_shutdown_sender, grpc_receiver, address),
        merger.start(merger_shutdown_sender),
    ) {
        Ok((_, _, _, _)) => {
            info!(log, "finished running server");
        }
        Err(err) => {
            error!(log, "a problem occurred"; "error" => format!("{:?}", err));
        }
    }

    Ok(())
}
