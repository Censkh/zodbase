---
title: Bun SQLite
description: Use Bun’s built-in SQLite driver with Zodbase, including local files, transactions, and connection cleanup.
---

Use `bun-sqlite` when your application runs on Bun. No separate SQLite driver package is required.

## Connect

```bash
bun add zodbase zod
```

```ts
import { Database as SQLite } from "bun:sqlite";
import { Database } from "zodbase";
import BunSqliteAdaptor from "zodbase/adaptors/bun-sqlite";

const driver = new SQLite(":memory:");
const db = new Database({ adaptor: new BunSqliteAdaptor({ driver }) });
```

Use a file path such as `"app.sqlite"` instead of `":memory:"` to persist data. See [getting started](/) for a complete table, insert, and query example.

## Transactions and writes

`db.transaction(async (tx) => { ... })` uses SQL `BEGIN`, `COMMIT`, and `ROLLBACK` on the same driver. Use `tx` for every operation in the callback; avoid unrelated work on that connection until it finishes. Nested transactions are rejected.

`updateMany` executes one update statement per row. Use an explicit transaction when those updates must succeed together.

## Schema changes and cleanup

The adaptor inherits [SQLite schema synchronization](/adaptors/sqlite/), including table rebuilding for supported constraint changes. Close the driver with `driver.close()` after outstanding operations finish. An in-memory database disappears when its connection closes.
