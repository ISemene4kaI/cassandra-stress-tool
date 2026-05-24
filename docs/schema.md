# Cassandra Schema

Apply this schema separately before migration and keep origin and target identical.

By default the app does not create schema. It only checks that `keyspace.events` is accessible. Use `APP_CREATE_SCHEMA=false` for ZDM rehearsal. Set `APP_CREATE_SCHEMA=true` only for local testing or throwaway environments.

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

If `CASSANDRA_KEYSPACE` or `CASSANDRA_LOCAL_DC` are changed and `APP_CREATE_SCHEMA=true`, the startup DDL uses those values.

The table is intentionally simple:

- partition key: `bucket`
- clustering key: `id`
- payload: random text sized by `APP_PAYLOAD_BYTES`
- `writer_id`: pod hostname or `local`
- `version`: fixed at `1`
