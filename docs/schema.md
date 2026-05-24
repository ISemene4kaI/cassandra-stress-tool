# Cassandra Schema

The app runs equivalent `CREATE IF NOT EXISTS` statements on startup.

```sql
CREATE KEYSPACE IF NOT EXISTS zdm_test
WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

CREATE TABLE IF NOT EXISTS zdm_test.events (
  bucket text,
  id uuid,
  created_at timestamp,
  payload text,
  writer_id text,
  version int,
  PRIMARY KEY ((bucket), id)
);
```

If `CASSANDRA_KEYSPACE` or `CASSANDRA_LOCAL_DC` are changed, the startup DDL uses those values.

The table is intentionally simple:

- partition key: `bucket`
- clustering key: `id`
- payload: random text sized by `APP_PAYLOAD_BYTES`
- `writer_id`: pod hostname or `local`
- `version`: fixed at `1`

