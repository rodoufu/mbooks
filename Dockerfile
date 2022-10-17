FROM rust:1.64.0-slim-buster AS builder
ADD . .
RUN cargo +$(cat rust-toolchain) test && cargo +$(cat rust-toolchain) build --release

FROM ubuntu:22.10 AS runtime
WORKDIR /app
COPY --from=builder target/release/mbooks .
ENTRYPOINT ["./mbooks"]
