# Runbook

## Local Run

```bash
cargo run --release
```

Example with explicit Cassandra contact points:

```bash
CASSANDRA_CONTACT_POINTS="127.0.0.1:9042" \
CASSANDRA_LOCAL_DC="dc1" \
APP_CREATE_SCHEMA="false" \
APP_RPS_PER_POD="100" \
APP_RECONNECT_AFTER_CONSECUTIVE_ERRORS="10" \
APP_READY_MAX_AGE_SECONDS="30" \
cargo run --release
```

HTTP endpoints:

```bash
curl -s localhost:8080/healthz
curl -i localhost:8080/readyz
curl -s localhost:8080/metrics
```

## Local Checks

```bash
cargo fmt -- --check
cargo check --locked
cargo clippy --locked -- -D warnings
docker build -t mini-cassandra-loadgen:ci .
```

The Docker context is trimmed by `.dockerignore`; docs, k8s manifests, Git metadata, and local build artifacts are not sent to the builder.

## Docker

```bash
docker build -t mini-cassandra-loadgen:latest .
docker run --rm \
  -p 8080:8080 \
  -e CASSANDRA_CONTACT_POINTS="host.docker.internal:9042" \
  -e CASSANDRA_LOCAL_DC="dc1" \
  mini-cassandra-loadgen:latest
```

## Kubernetes

Edit `k8s/deployment.yaml` and set `CASSANDRA_CONTACT_POINTS` for the current migration phase.
`APP_RPS_PER_POD` is per replica; with the default `replicas: 2`, total intended traffic is `2 * APP_RPS_PER_POD`.
For rehearsal, pin the image to an immutable tag such as `ghcr.io/isemene4kai/cassandra-stress-tool:v1.0.4`. Use `latest` only for development.

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/servicemonitor.yaml
```

Check rollout and readiness:

```bash
kubectl rollout status deploy/mini-cassandra-loadgen
kubectl get pods -l app=mini-cassandra-loadgen
kubectl logs -l app=mini-cassandra-loadgen --tail=100
```

Port-forward metrics:

```bash
kubectl port-forward svc/mini-cassandra-loadgen 8080:8080
curl -s localhost:8080/metrics
```

## Migration Flow

1. Run against origin VM Cassandra and establish a clean baseline.
2. Switch `CASSANDRA_CONTACT_POINTS` to ZDM Proxy.
3. Watch metrics and logs during dual-write/read routing phases.
4. Switch `CASSANDRA_CONTACT_POINTS` to target Kubernetes Cassandra.
5. Keep the app running long enough to catch intermittent routing, DNS, auth, TLS, or consistency issues.

The expected success state is:

- `miniapp_read_errors_total` does not increase
- `miniapp_write_errors_total` does not increase
- `miniapp_reads_empty_total{read_source="recent"}` does not increase
- `miniapp_last_success_timestamp` remains fresh
- `/readyz` stays `200`

If `/readyz` returns `503`, the body is a reason such as `cassandra_client_not_ready`, `connect_failed`, `schema_check_failed`, `prepare_failed`, `too_many_consecutive_errors`, or `cassandra_operations_stale`.
