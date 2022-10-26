# mbooks

This application implements a gRPC server and client that are configurable using CLI subcommands `server` and `client` respectively.

The application has this main components:
- `OrderbookMerger` it keeps the book information consolidated and produces a `Summary` message every time it gets an update.
It keeps a channel that will listen to messages from the `Source`s.
- `Source`, each source will be a websocket implementation that will listen to a Exchange parse the updates and send it in the expected format. 
The `Source` receives a channel to which it is going to send all the updates it produces. 
In this case was implemented `run_binance` for Binance and `run_bitstamp` for Bitstamp.
- `Server`, the implementation of the gRPC server that will listen to requests and stream the Summary updates.
The `OrderbookAggregatorImpl` keeps a list of `ClientSubscription` which is a channel to send the summaries, so on
every update it gets, it is going to send it to all the subscribed clients.
- `Client`, the gRPC client implementation who will make a request and listen to the Summary updates and print them.

The service implements a graceful shutdown that listens to the `Ctrl + C` commands and propagates it to all services using a channel.
The usage of graceful stop can be very important in scenarios where it is necessary to do something once the service is closed.
For example, if it is a bot trading it may be necessary to close all the open orders instead of just shutting down and leaving they all open.

The service is using a simple configuration of `slog` as the structured logger, which is a good option to add metadata to the log messages.
It also uses OpenTelemetry to add some tracing information to application, it could also make use of metrics such as Prometheus counter to help in the monitoring of the service.

There is a simple docker compose services configuration to demonstrate a server being created and 10 clients listening to it.

To deserialize the JSON from the exchanges it is using Serde, it may have faster options, but I did not want to focus on this kind of optimization.
The main goal was to focus on the service itself and managing the orders from the book.

In order to generate the Rust code from the `proto` definitions it was chosen to create a `build.rs` and use `tonic` to build it
since it was going to be tue one used to run the gRPC server.
A good tool to use here is `bufbuild`, which has the ability to generate code from the `proto` definition and also provides a `lint` tool with some good practices.

It is possible to configure the application and choose the pair that is going to be streamed by the server.
Although, each side of the pair needs to match an enumeration maintained internally.
It was designed like that, so it can be translated to distinct market formats across new exchanges that may be added and
it is also used as a safeguard, to avoid problems with typos like configuring `ETC` instead of `ETH`.
It expects the markets in the format `eth/btc`, then it translates to the format each exchange expects.
It was designed like that (with the `/`) to simplify using assets with 3 or 4 characters.

## OrderbookMerger

Considering that:
- `k` is the depth tracked from the book.
- `e` is the number of exchanges.

It doesn't need to keep all the book from the `Source`
So the `Source` is sending only the `k` registers from the Orderbook in a sorted vector.
This way the merger leverages that the updates are sorted, so it can use the merge step of the merge sort to just keep the `k` registers (from the current exchange) it needs,
which would cost `O(e k)` comparisons.

It keeps `k` orders for each exchange, so `e * k` in total, that would change the time complexity to `O(e k)` where e is the number of exchanges, 
but since e is a constant number the complexity will still be in linear time to the size of the book.

A benchmark was written using `criterion-rs` to compare the performance of the `OrderbookMerger` after removing some `Clone` operations.
Here follows the results with the performance improvement:
```shell

running 10 tests
test binance::test::should_convert_symbol ... ignored
test binance::test::should_parse_data ... ignored
test bitstamp::test::should_convert_symbol ... ignored
test bitstamp::test::should_parse_a_subscribe ... ignored
test bitstamp::test::should_parse_data ... ignored
test merger::test::should_add_empty_summary_to_an_existing_orderbook ... ignored
test merger::test::should_add_to_an_empty_orderbook ... ignored
test merger::test::should_add_to_an_existing_orderbook ... ignored
test merger::test::should_replace_outdated_data_from_same_exchange ... ignored
test types::test::should_parse_ethbtc_pair ... ignored

test result: ok. 0 passed; 0 failed; 10 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

merger merging 2 objects
                        time:   [3.4997 µs 3.5044 µs 3.5094 µs]
                        change: [-16.091% -15.269% -14.737%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe

merger merging 5 objects
                        time:   [5.1526 µs 5.2316 µs 5.3429 µs]
                        change: [-42.162% -41.486% -40.473%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 14 outliers among 100 measurements (14.00%)
  4 (4.00%) high mild
  10 (10.00%) high severe

merger merging 10 objects
                        time:   [7.5864 µs 7.6476 µs 7.7186 µs]
                        change: [-49.831% -49.511% -49.132%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild

merger merging 20 objects
                        time:   [12.141 µs 12.177 µs 12.218 µs]
                        change: [-58.283% -58.053% -57.839%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 10 outliers among 100 measurements (10.00%)
  8 (8.00%) high mild
  2 (2.00%) high severe

merger merging 50 objects
                        time:   [25.920 µs 25.965 µs 26.018 µs]
                        change: [-62.530% -62.110% -61.827%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 8 outliers among 100 measurements (8.00%)
  4 (4.00%) high mild
  4 (4.00%) high severe

merger merging 100 objects

                        time:   [48.913 µs 49.073 µs 49.316 µs]
                        change: [-62.402% -62.191% -61.947%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 15 outliers among 100 measurements (15.00%)
  1 (1.00%) low mild
  4 (4.00%) high mild
  10 (10.00%) high severe

merger merging 200 objects
                        time:   [92.948 µs 93.121 µs 93.304 µs]
                        change: [-63.934% -63.829% -63.741%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) high mild
  1 (1.00%) high severe

merger merging 500 objects
                        time:   [226.56 µs 229.02 µs 232.24 µs]
                        change: [-63.299% -63.039% -62.656%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 13 outliers among 100 measurements (13.00%)
  5 (5.00%) high mild
  8 (8.00%) high severe
```
