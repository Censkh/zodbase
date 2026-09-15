---
title: CockroachDB
description: Connect CockroachDB through pg and understand Zodbase transaction retries, locality, and primary-key changes.
---

The CockroachDB adaptor extends the PostgreSQL adaptor and uses `pg`. Select it explicitly to get CockroachDB-specific retries and schema behavior.

## Install and connect

```bash
npm install zodbase zod pg
```

```ts
import pg from "pg";
import { Database } from "zodbase";
import CockroachAdaptor from "zodbase/adaptors/cockroach";

const driver = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const db = new Database({ adaptor: new CockroachAdaptor({ driver }) });
```

Configure TLS and credentials on the driver for your cluster. Close the pool with `await driver.end()` when the application shuts down.

## Transaction retries

Serialization failures with SQLSTATE `40001` cause the entire transaction callback to run again, for at most three attempts. The adaptor rolls back before retrying and uses a short exponential delay with jitter. Other errors are propagated without this retry.

Use `tx` for database work inside the callback. Send emails, publish messages, and perform other external effects after the transaction succeeds, since callback code may execute more than once.

## Locality and schema behavior

The adaptor recognizes `cockroachLocality` metadata on the table's object schema and `cockroachRegion` metadata on a region field. Supported locality kinds are `global`, `regional-by-table`, and `regional-by-row`. A custom regional-by-row column must exist and carry region metadata. Configure the database's regions before synchronizing table locality.

Primary-key synchronization uses `ALTER PRIMARY KEY` and can replace CockroachDB's implicit primary key. Replacing a different explicit primary key is outside automatic synchronization's contract.

For shared driver options and diagnostics, see [PostgreSQL](/adaptors/postgres/); for callback semantics, see [transactions](/transactions/).
