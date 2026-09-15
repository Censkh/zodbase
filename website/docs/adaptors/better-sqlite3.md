---
title: better-sqlite3
description: Connect Zodbase to better-sqlite3 and manage synchronous SQLite execution, transactions, and driver lifetime.
---

Use this adaptor with an initialized `better-sqlite3` database.

## Install and connect

```bash
npm install zodbase zod better-sqlite3
```

```ts
import SQLite from "better-sqlite3";
import { Database } from "zodbase";
import BetterSqlite3Adaptor from "zodbase/adaptors/better-sqlite3";

const driver = new SQLite("app.sqlite");
const db = new Database({ adaptor: new BetterSqlite3Adaptor({ driver }) });
```

The driver must expose `prepare()`, with statements supporting `reader`, `all()`, and `run()`. The adaptor uses `all()` for statements that return rows and `run()` for other writes. Driver calls are synchronous even though Zodbase exposes an asynchronous API.

## Transactions and schema changes

Use `db.transaction()` for atomic groups of operations and use the callback's `tx` instance inside it. The adaptor inherits SQL transaction handling from the base implementation; nested transactions are rejected. `updateMany` runs statements sequentially and needs an explicit transaction for all-or-nothing behavior.

See the [SQLite base guide](/adaptors/sqlite/) for schema rebuilding and foreign keys.

## Cleanup

Keep the driver open while the application uses it, then call `driver.close()` after pending work finishes. See the [better-sqlite3 driver reference](https://github.com/WiseLibs/better-sqlite3/blob/master/docs/api.md) for driver options and native installation details.
