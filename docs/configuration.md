# Configuration

The app is configured only through environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `CASSANDRA_CONTACT_POINTS` | `127.0.0.1:9042` | Comma-separated Cassandra, ZDM Proxy, or target k8s Cassandra `host:port` list. |
| `CASSANDRA_LOCAL_DC` | `dc1` | Local datacenter for driver load balancing and keyspace replication. |
| `CASSANDRA_KEYSPACE` | `zdm_test` | Keyspace used by the app. |
| `CASSANDRA_USERNAME` | empty | Optional Cassandra username. |
| `CASSANDRA_PASSWORD` | empty | Optional Cassandra password. |
| `CASSANDRA_CONSISTENCY` | `LOCAL_QUORUM` | One of `ONE`, `LOCAL_ONE`, `QUORUM`, `LOCAL_QUORUM`. |
| `CASSANDRA_TLS_ENABLED` | `false` | Validated flag. `true` fails fast because CA/cert envs are intentionally not part of this mini-app. |
| `APP_CREATE_SCHEMA` | `false` | If `true`, creates keyspace/table. If `false`, only checks that schema exists. |
| `APP_RPS_PER_POD` | `1000` | Approximate operation rate per pod. Total rate is `APP_RPS_PER_POD * replicas`. |
| `APP_READ_RATIO` | `70` | Relative read weight. |
| `APP_WRITE_RATIO` | `30` | Relative write weight. |
| `APP_PAYLOAD_BYTES` | `4096` | Random payload size for each write. |
| `APP_WORKERS` | `32` | Number of async workers. |
| `APP_BUCKETS` | `256` | Number of logical partition buckets. |
| `APP_HISTORICAL_READ_ENABLED` | `false` | Enables historical read mode. Without a known id list, reads are labeled `random_miss_probe`. |
| `APP_HISTORICAL_BUCKETS` | `APP_BUCKETS` | Bucket range for `random_miss_probe`. |
| `APP_METRICS_ADDR` | `0.0.0.0:8080` | HTTP bind address for health, readiness, and metrics. |
| `APP_LOG_EVERY_N_SUCCESS` | `1000` | Emit compact success stats after this many successful operations. Set `0` to disable. |
| `RUST_LOG` | `info` | Tracing filter. |

Legacy `APP_RPS` is accepted as fallback when `APP_RPS_PER_POD` is unset.

## Contact Point Switching

Use the same deployment and change only `CASSANDRA_CONTACT_POINTS`.

Origin VM Cassandra:

```yaml
CASSANDRA_CONTACT_POINTS: "vm-cassandra-1.example.com:9042,vm-cassandra-2.example.com:9042"
```

ZDM Proxy:

```yaml
CASSANDRA_CONTACT_POINTS: "zdm-proxy.datastax.svc.cluster.local:9042"
```

Target Kubernetes Cassandra:

```yaml
CASSANDRA_CONTACT_POINTS: "cassandra-dc1-service.cassandra.svc.cluster.local:9042"
```
