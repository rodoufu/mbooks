FROM rust:1.64.0-slim-buster AS builder
RUN apt-get update && apt-get install -y protobuf-compiler libssl-dev pkg-config
WORKDIR /app
ADD . .
RUN cargo +$(cat rust-toolchain) test && cargo +$(cat rust-toolchain) build --release

FROM ubuntu:22.10 AS runtime
RUN apt-get update && apt-get install -y libssl-dev wget && \
    wget http://nz2.archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2.16_amd64.deb && \
    dpkg -i libssl1.1_1.1.1f-1ubuntu2.16_amd64.deb

WORKDIR /app
COPY --from=builder app/target/release/mbooks .
ENTRYPOINT ./mbooks
