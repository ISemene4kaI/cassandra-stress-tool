# mini-cassandra-loadgen

Minimal Rust mini-app for Cassandra VM -> Kubernetes migration rehearsal through DataStax ZDM Proxy.

This is a downtime detector, not a benchmark. It continuously writes and reads a small Cassandra table, keeps running through Cassandra/ZDM outages, logs read/write problems, and exposes Prometheus metrics so migration phases can be checked for visible downtime or data anomalies.

Main success criteria:

- `miniapp_read_errors_total` stays `0`
- `miniapp_write_errors_total` stays `0`
- `miniapp_reads_empty_total{read_source="recent"}` stays `0`
- `miniapp_last_success_timestamp` stays fresh
- `/readyz` stays `200`

## Docs

- [Configuration](docs/configuration.md)
- [Cassandra schema](docs/schema.md)
- [Runbook](docs/runbook.md)
- [Observability](docs/observability.md)

## Environment

| Variable | Default | Description |
| --- | --- | --- |
| `CASSANDRA_CONTACT_POINTS` | `127.0.0.1:9042` | Comma-separated contact points for VM Cassandra, ZDM Proxy, or target k8s Cassandra. |
| `CASSANDRA_LOCAL_DC` | `dc1` | Local datacenter for load balancing and schema DDL when enabled. |
| `CASSANDRA_KEYSPACE` | `zdm_test` | Keyspace containing `events`. |
| `CASSANDRA_USERNAME` | empty | Optional username. |
| `CASSANDRA_PASSWORD` | empty | Optional password. |
| `CASSANDRA_CONSISTENCY` | `LOCAL_QUORUM` | `ONE`, `LOCAL_ONE`, `QUORUM`, or `LOCAL_QUORUM`. |
| `CASSANDRA_TLS_ENABLED` | `false` | Validated only. `true` fails fast because cert envs are not part of this mini-app. |
| `APP_CREATE_SCHEMA` | `false` | If `true`, creates keyspace/table. If `false`, only checks that schema exists. |
| `APP_RPS_PER_POD` | `1000` | Target operations per second per pod. Total rate is this value multiplied by replicas. |
| `APP_READ_RATIO` | `70` | Relative read weight. |
| `APP_WRITE_RATIO` | `30` | Relative write weight. |
| `APP_PAYLOAD_BYTES` | `4096` | Random payload size for writes. |
| `APP_WORKERS` | `32` | Number of async workers per pod. |
| `APP_BUCKETS` | `256` | Bucket range for writes and recent reads. |
| `APP_HISTORICAL_READ_ENABLED` | `false` | Enables explicit historical-read mode, currently implemented as `random_miss_probe` without a known id list. |
| `APP_HISTORICAL_BUCKETS` | `APP_BUCKETS` | Bucket range for `random_miss_probe`. |
| `APP_RECONNECT_AFTER_CONSECUTIVE_ERRORS` | `10` | Reset the Cassandra session after this many consecutive operation errors. `0` disables operation-error session reset. |
| `APP_READY_MAX_AGE_SECONDS` | `30` | `/readyz` freshness window for the last successful Cassandra operation. |
| `APP_METRICS_ADDR` | `0.0.0.0:8080` | HTTP bind address. |
| `APP_LOG_EVERY_N_SUCCESS` | `1000` | Compact stats log interval. `0` disables. |

Legacy `APP_RPS` is accepted as fallback if `APP_RPS_PER_POD` is unset.

## Schema

Apply schema separately before migration and keep origin and target identical. The app does not create schema by default.

See [docs/schema.md](docs/schema.md).

## Kubernetes

Set the current migration endpoint in [k8s/deployment.yaml](k8s/deployment.yaml):

```yaml
CASSANDRA_CONTACT_POINTS: "zdm-proxy.datastax.svc.cluster.local:9042"
APP_CREATE_SCHEMA: "false"
APP_RPS_PER_POD: "1000"
```

Apply:

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/servicemonitor.yaml
```

## Migration Contact Points

Origin VM Cassandra:

```yaml
CASSANDRA_CONTACT_POINTS: "vm-cassandra-1.example.com:9042,vm-cassandra-2.example.com:9042"
```

ZDM Proxy:

```yaml
CASSANDRA_CONTACT_POINTS: "zdm-proxy.datastax.svc.cluster.local:9042"
```

Target k8s Cassandra:

```yaml
CASSANDRA_CONTACT_POINTS: "cassandra-dc1-service.cassandra.svc.cluster.local:9042"
```

## Problems To Treat As Migration Issues

- Any increase in `miniapp_read_errors_total`
- Any increase in `miniapp_write_errors_total`
- Any recently written key returning empty: `miniapp_reads_empty_total{read_source="recent"} > 0`
- `time() - miniapp_last_success_timestamp > 30`
- sustained latency increase in `miniapp_operation_latency_seconds`

`random_miss_probe` empty reads are expected and do not prove historical data migration success. They only prove Cassandra answered a read request.
