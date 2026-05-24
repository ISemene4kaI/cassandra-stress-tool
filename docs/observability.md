# Observability

## Metrics

| Metric | Type | Meaning |
| --- | --- | --- |
| `miniapp_reads_total` | counter | Successful read operations. |
| `miniapp_writes_total` | counter | Successful write operations. |
| `miniapp_read_errors_total` | counter | Failed read operations. |
| `miniapp_write_errors_total` | counter | Failed write operations. |
| `miniapp_operation_latency_seconds` | histogram | Read/write operation latency, labeled by `operation`. |
| `miniapp_last_success_timestamp` | gauge | Unix timestamp of the last successful Cassandra operation. |
| `miniapp_inflight_operations` | gauge | Operations currently waiting on Cassandra. |
| `miniapp_cassandra_reconnects_total` | counter | Initial successful Cassandra connection count. |

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

Every `APP_LOG_EVERY_N_SUCCESS` successful operations, the app logs compact counters:

- `reads_ok`
- `reads_failed`
- `writes_ok`
- `writes_failed`
- `current_rps`

