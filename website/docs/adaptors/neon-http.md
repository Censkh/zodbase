---
title: Neon HTTP
description: Use Neon's HTTP driver for PostgreSQL queries and scalar subquery inserts.
---

```bash
npm install zodbase zod @neondatabase/serverless
```

```ts
import { neon } from "@neondatabase/serverless";
import { Database } from "zodbase";
import NeonHttpAdaptor from "zodbase/adaptors/neon-http";

const driver = neon(process.env.DATABASE_URL!);
const db = new Database({ adaptor: new NeonHttpAdaptor({ driver }) });
```

This adaptor uses Neon's `query()` HTTP API and explicitly requests object rows with full results, even if the driver was configured with array mode. PostgreSQL SQL generation, scalar subquery inserts, `RETURNING` and result decoding are shared with the PostgreSQL adaptor. Drivers remain application dependencies and are not bundled by Zodbase.

A subquery insert remains one HTTP request. `db.transaction(callback)` is rejected **before the callback runs**: HTTP requests do not share an interactive transaction session. Schema synchronization can issue several independent requests, so do migrations through a session-capable connection when atomicity is required.

For interactive transactions, use Neon's WebSocket pool with the existing PostgreSQL adaptor:

```ts
import { Pool } from "@neondatabase/serverless";
import PostgresAdaptor from "zodbase/adaptors/postgres";

const driver = new Pool({ connectionString: process.env.DATABASE_URL });
const db = new Database({ adaptor: new PostgresAdaptor({ driver }) });
// await driver.end() at shutdown
```

The PostgreSQL adaptor accepts the shared structural query/pool interface, checked against both actual driver packages. Configure the driver's WebSocket implementation for runtimes that require one. See [Neon's driver guide](https://github.com/neondatabase/serverless#readme).

Validation includes the real Neon HTTP driver's request/response parsing with an intercepted fetch transport, plus PostgreSQL SQL integration tests. It does not provision or test a hosted Neon account.
