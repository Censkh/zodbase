---
title: Turso and libSQL
description: Use a libSQL client with Zodbase for local files or Turso, including write transactions and batching.
---

Use `@libsql/client` for a local libSQL database or a remote Turso database.

## Install and connect

```bash
npm install zodbase zod @libsql/client
```

```ts
import { createClient } from "@libsql/client";
import { Database } from "zodbase";
import TursoAdaptor from "zodbase/adaptors/turso";

const driver = createClient({
  url: process.env.TURSO_DATABASE_URL!,
  authToken: process.env.TURSO_AUTH_TOKEN,
});
const db = new Database({ adaptor: new TursoAdaptor({ driver }) });
```

For local development, use a file URL such as `"file:app.db"` and omit the remote authentication token. Keep remote credentials on the server.

## Transactions and batches

`db.transaction()` opens a libSQL `"write"` transaction. Operations on the callback's `tx` instance use that transaction. It commits on success, rolls back on error, and closes the transaction handle in either case. Nested transactions are rejected.

`updateMany` sends the generated updates through `driver.batch(statements, "write")`. Reads map the driver's `rows` into Zodbase's result shape.

## Schema changes and cleanup

The adaptor inherits [SQLite schema synchronization](/adaptors/sqlite/). Confirm that your libSQL deployment supports the schema operations you intend to apply.

Call `driver.close()` after outstanding application work completes. The adaptor closes its own transaction handles, but the application owns the client.
