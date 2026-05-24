FROM rust:1-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim

ARG VERSION=1.0.4
ARG REVISION=unknown
ARG SOURCE=https://github.com/ISemene4kaI/cassandra-stress-tool

LABEL org.opencontainers.image.title="mini-cassandra-loadgen"
LABEL org.opencontainers.image.description="Rust mini-app for Cassandra VM to Kubernetes migration rehearsal through ZDM Proxy"
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.revision=$REVISION
LABEL org.opencontainers.image.source=$SOURCE

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --create-home app
USER app
WORKDIR /app

COPY --from=builder /app/target/release/mini-cassandra-loadgen /usr/local/bin/mini-cassandra-loadgen

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/mini-cassandra-loadgen"]
