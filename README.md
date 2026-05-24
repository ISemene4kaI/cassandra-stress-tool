# mini-cassandra-loadgen

Minimal Rust load generator and downtime detector for Cassandra VM to Kubernetes migration testing through DataStax ZDM Proxy.

It runs in Kubernetes, continuously writes and reads Cassandra, exposes Prometheus metrics, logs Cassandra errors as JSON, and keeps running when Cassandra operations fail.

Main success criterion during migration: read/write error counters stay at `0`, and `miniapp_last_success_timestamp` stays fresh.

## Docs

- [Configuration](docs/configuration.md)
- [Cassandra schema](docs/schema.md)
- [Runbook](docs/runbook.md)
- [Observability](docs/observability.md)

## Project Layout

```text
.
├── Cargo.toml
├── Dockerfile
├── README.md
├── docs/
├── k8s/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── servicemonitor.yaml
└── src/
    └── main.rs
```

## Quick Start

```bash
CASSANDRA_CONTACT_POINTS="127.0.0.1:9042" \
CASSANDRA_LOCAL_DC="dc1" \
APP_RPS="100" \
cargo run --release
```

Endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

## Kubernetes

Set the current migration endpoint in `k8s/deployment.yaml`:

```yaml
CASSANDRA_CONTACT_POINTS: "zdm-proxy.datastax.svc.cluster.local:9042"
```

Apply:

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/servicemonitor.yaml
```

See the [runbook](docs/runbook.md) for local, Docker, Kubernetes, and migration usage.

