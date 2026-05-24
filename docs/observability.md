# Observability

## Metrics

| Metric | Type | Meaning |
| --- | --- | --- |
| `miniapp_reads_total` | counter | Successful read operations. |
| `miniapp_reads_found_total` | counter | Reads that returned at least one row, labeled by `read_source`. |
| `miniapp_reads_empty_total` | counter | Reads that returned no rows, labeled by `read_source`. Recent empty reads are anomalies. |
| `miniapp_writes_total` | counter | Successful write operations. |
| `miniapp_read_errors_total` | counter | Failed read operations and recent-key empty anomalies, labeled by `read_source` when available. |
| `miniapp_write_errors_total` | counter | Failed write operations. |
| `miniapp_operation_latency_seconds` | histogram | Read/write operation latency, labeled by `operation`. |
| `miniapp_last_success_timestamp` | gauge | Unix timestamp of the last successful Cassandra operation. |
| `miniapp_inflight_operations` | gauge | Operations currently waiting on Cassandra. |
| `miniapp_cassandra_connect_attempts_total` | counter | Cassandra/ZDM connection attempts. |
| `miniapp_cassandra_connects_total` | counter | Successful Cassandra/ZDM connections, including reconnects. |
| `miniapp_cassandra_ready` | gauge | `1` when a session is connected, schema checked, and statements prepared. |

## Alert Examples

```yaml
groups:
  - name: mini-cassandra-loadgen
    rules:
      - alert: MiniCassandraLoadgenReadErrors
        expr: increase(miniapp_read_errors_total[1m]) > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: Cassandra read errors during migration test

      - alert: MiniCassandraLoadgenWriteErrors
        expr: increase(miniapp_write_errors_total[1m]) > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: Cassandra write errors during migration test

      - alert: MiniCassandraLoadgenNoFreshSuccess
        expr: time() - miniapp_last_success_timestamp > 30
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: No successful Cassandra operation in the last 30 seconds

      - alert: MiniCassandraLoadgenRecentReadEmpty
        expr: increase(miniapp_reads_empty_total{read_source="recent"}[1m]) > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: Recently written Cassandra row was not found
```

## Logs

Logs are JSON to stdout.

Every Cassandra error includes:

- `operation`
- `consistency`
- `error_type`
- `error`
- `latency_ms`
- `contact_points`
- `keyspace`
- `timestamp`

Recent-key empty reads are logged as errors because they mean a row written by this app could not be read back. `random_miss_probe` empty reads are counted in `miniapp_reads_empty_total` and should not be interpreted as data migration success.

The compact success log fields are:

- `reads_found`
- `reads_empty`
- `reads_failed`
- `writes_ok`
- `writes_failed`
- `current_rps`

## Metrics Upkeep

This project uses `metrics-exporter-prometheus` with `install_recorder()` and serves `/metrics` from Axum. In the pinned crate version, `PrometheusHandle` does not expose a public `run_upkeep()` method. A small background task periodically calls `render()`, which uses the same snapshot path as `/metrics` and keeps histogram/counter exposition stable between scrapes.
