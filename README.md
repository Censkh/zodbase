# zodbase

Schema-driven database utilities used by CalmLens. The package provides typed tables and queries, database adaptors, statement helpers, and conversion of a supported RSQL subset into query conditions.

It is currently an internal workspace package and does not provide a stable public API. See `src/index.ts` for exports and `tests/rsql.test.ts` for supported filter syntax.

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
