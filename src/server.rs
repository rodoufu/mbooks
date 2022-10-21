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

pub struct OrderbookAggregatorImpl {
    log: Logger,
    clients_to_connect_sender: Sender<Sender<Result<Summary, Status>>>,
}

impl OrderbookAggregatorImpl {
    fn new(
        log: Logger,
        clients_to_connect_sender: Sender<Sender<Result<Summary, Status>>>,
    ) -> Self {
        Self {
            log,
            clients_to_connect_sender,
        }
    }

    async fn listen_clients_to_connect(
        targets: &Mutex<Vec<Sender<Result<Summary, Status>>>>,
        clients_to_connect_receiver: &Mutex<Receiver<Sender<Result<Summary, Status>>>>,
    ) -> Result<(), tonic::transport::Error> {
        let mut clients_to_connect_receiver = clients_to_connect_receiver.lock().await;
        while let Some(client_to_connect) = clients_to_connect_receiver.recv().await {
            targets.lock().await.push(client_to_connect);
        }
        Ok(())
    }

    async fn listen_summaries(
        log: Logger,
        targets: &Mutex<Vec<Sender<Result<Summary, Status>>>>,
        grpc_receiver: UnboundedReceiver<Summary>,
    ) -> Result<(), tonic::transport::Error> {
        let mut grpc_receiver = grpc_receiver;
        while let Some(summary) = grpc_receiver.recv().await {
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
        }
        Ok(())
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

async fn run_grpc_server(
    log: Logger,
    grpc_receiver: UnboundedReceiver<Summary>, port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let tracer = global::tracer("run_server");
    let span = tracer.start(format!("running server at: {}", port));
    let cx = Context::current_with_span(span);

    let addr = format!("[::1]:{}", port).parse()
        .map_err(|e| format!("problem parsing address: {}", e))?;

    let targets = Mutex::new(Vec::new());
    let (clients_to_connect_sender, clients_to_connect_receiver) = mpsc::channel(10);
    let clients_to_connect_receiver = Mutex::new(clients_to_connect_receiver);
    let orderbook = OrderbookAggregatorImpl::new(
        log.clone(),
        clients_to_connect_sender,
    );

    info!(log, "Orderbook server listening"; "address" => addr);

    let run_grpc_server = Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook))
        .serve(addr)
        .with_context(cx);

    tokio::try_join!(
        OrderbookAggregatorImpl::listen_summaries(
            log.clone(),
            &targets,
            grpc_receiver,
        ),
        OrderbookAggregatorImpl::listen_clients_to_connect(
            &targets,
            &clients_to_connect_receiver,
        ),
        run_grpc_server,
    )?;

    Ok(())
}

pub async fn run_server(
    log: Logger, port: u16, pair: Symbol, depth: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (summary_sender, summary_receiver) = mpsc::unbounded_channel();
    let (grpc_sender, grpc_receiver) = mpsc::unbounded_channel();

    let mut merger = OrderbookMerger::new(
        log.clone(), summary_receiver, grpc_sender, depth,
    );

    match tokio::try_join!(
        run_binance(log.clone(), summary_sender.clone(), &pair, depth),
        run_bitstamp(log.clone(), summary_sender, &pair, depth),
        run_grpc_server(log.clone(), grpc_receiver, port),
        merger.start(),
    ) {
        Ok((_, _, _, _)) => {}
        Err(err) => {
            error!(log, "a problem occurred"; "error" => format!("{:?}", err));
        }
    }

    Ok(())
}
