# zodbase

Schema-driven database utilities used by CalmLens. The package provides typed tables and queries, database adaptors, statement helpers, and conversion of a supported RSQL subset into query conditions.

It is currently an internal workspace package and does not provide a stable public API. See `src/index.ts` for exports and `tests/rsql.test.ts` for supported filter syntax.

## Schema synchronization

`database.syncTable(table)` adds a missing primary key when an existing column gains `primaryKey()` metadata. Sync is repeatable, and existing rows are preserved. Duplicate values are rejected rather than deleted or rewritten. Nullable columns still require a backfill before becoming required when null values exist.

PostgreSQL and MySQL-compatible adaptors add a primary-key constraint; SQLite-compatible adaptors rebuild the table; CockroachDB replaces its implicit primary key. Replacing a different explicit primary key is not supported by automatic sync.

## Lazy adaptor initialization

Pass an initializer instead of an adaptor instance to defer setup until the first database operation executes. The initializer is memoized and runs at most once per `Database` instance.

```ts
const database = new Database({
  adaptor: async () => {
    const driver = await connect();
    return new PostgresAdaptor({ driver });
  },
});
```
