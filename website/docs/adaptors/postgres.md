---
title: PostgreSQL
description: Connect Zodbase through pg, use pooled transactions, and manage PostgreSQL driver diagnostics.
---

Use the `pg` driver with a pool or a connected client.

## Install and connect

```bash
npm install zodbase zod pg
```

```ts
import pg from "pg";
import { Database } from "zodbase";
import PostgresAdaptor from "zodbase/adaptors/postgres";

const driver = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const db = new Database({ adaptor: new PostgresAdaptor({ driver }) });
```

Keep credentials and connection options in the driver configuration. Zodbase does not manage your connection string or pool size. A directly supplied `pg.Client` must already be connected.

## Transactions

For a pool, the adaptor checks out one client for the entire transaction and releases it in `finally`. Use the callback's `tx` instance so all operations stay on that client. A successful callback commits; a thrown error rolls back. Nested transactions are rejected.

## Types and schema changes

JavaScript numbers use `DOUBLE PRECISION` rather than `REAL`. JSON array membership uses PostgreSQL's JSONB containment operator. Typed reads apply Zodbase's [result decoding](/results-and-validation/).

`syncTable` can add a primary-key constraint to an existing column. Duplicate values cause an error; automatic synchronization does not replace a different explicit primary key. Read [tables and schemas](/tables-and-schemas/) before changing populated tables.

## Diagnostics and cleanup

Configure `events.onExecuteStatement` on the adaptor to receive execution diagnostics; see [configuration](/configuration/). Close the pool with `await driver.end()` during application shutdown, after pending operations finish.
