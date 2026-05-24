FROM rust:1-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --create-home app
USER app
WORKDIR /app

COPY --from=builder /app/target/release/mini-cassandra-loadgen /usr/local/bin/mini-cassandra-loadgen

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/mini-cassandra-loadgen"]
