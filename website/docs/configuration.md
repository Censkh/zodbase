---
title: Runtime and configuration
description: Configure Zodbase database instances, lazy initialization, driver lifetimes, runtime imports, and timestamp metadata.
---

## Create a database instance

```ts
const db = new Database({
  adaptor: new BunSqliteAdaptor({ driver }),
});
```

The adaptor is required. The application owns the driver, connection configuration, credentials, and cleanup. For example, close Bun SQLite with `driver.close()` or a PostgreSQL pool with its own driver API when your application is finished.

## Initialize lazily

```ts
const db = new Database({
  adaptor: async () => {
    const driver = await connect();
    return new PostgresAdaptor({ driver });
  },
});
```

The initializer is shared across concurrent first operations and runs at most once per `Database` instance. `connect()` is your own factory. A failed initialization is also memoized; construct a new instance if your application needs to retry initialization.

## Server and browser imports

The normal package entry point is for database execution in server and supported native runtimes. Its browser conditional export contains metadata utilities and execution stubs, not a browser database client. Keep database execution on the server; use the `expo-sqlite` adaptor for the supported native SQLite integration.

Import database adaptors from their dedicated `zodbase/adaptors/...` entry points. Import RSQL separately from `zodbase/rsql` and install `@rsql/parser` when using it.

## Update timestamps

```ts
import { metaStore, monotonicTimestamp, updatedAt } from "zodbase";

const updated = z.number().meta(metaStore([updatedAt()]));
const revision = z.number().meta(metaStore([monotonicTimestamp()]));
```

`updatedAt()` assigns a current timestamp during the ordinary `db.update` path. `monotonicTimestamp()` advances the stored numeric value even when the current clock would not be greater. Supply initial values or schema defaults on insertion. Do not assume these update behaviors run for raw SQL, bulk updates, or upserts; they are implemented in `db.update`.

## Diagnostics

PostgreSQL and MySQL adaptor execution paths expose `events.onExecuteStatement`. Configure it on the adaptor itself:

```ts
const adaptor = new PostgresAdaptor({
  driver,
  events: {
    onExecuteStatement: ({ success, timings }) => {
      console.log({ success, elapsedMs: timings.wallTimeMs });
    },
  },
});
```

The event also contains SQL text; avoid logging sensitive values. The similarly named options on `Database` are not automatically forwarded into an adaptor instance you constructed. Driver errors are propagated; application code decides how to classify and retry them.
