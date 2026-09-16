---
title: MySQL and MariaDB
description: Configure mysql2 for Zodbase, pooled transactions, SQL modes, and MySQL-specific schema constraints.
---

Use the promise API from `mysql2` with a pool or an established connection.

## Install and connect

```bash
npm install zodbase zod mysql2
```

```ts
import mysql from "mysql2/promise";
import { Database } from "zodbase";
import MysqlAdaptor from "zodbase/adaptors/mysql";

const driver = mysql.createPool(process.env.DATABASE_URL!);
const db = new Database({ adaptor: new MysqlAdaptor({ driver }) });
```

## Sessions and transactions

The adaptor configures `NO_BACKSLASH_ESCAPES` on the connection because SQL string literals use quote doubling. Account for this session mode if the same connection also runs SQL outside Zodbase.

A pooled transaction checks out one connection and releases it after commit or rollback. Use the transaction's `tx` database for every operation in the callback. Nested transactions are rejected.

## Database-specific behavior

- Strings map to `VARCHAR(255)` by default.
- Numbers map to `DOUBLE`, dates to `DATETIME(3)`, and objects/arrays to `JSON`.
- JSON array membership uses `JSON_CONTAINS`.
- Partial indexes are rejected.
- Mutation helpers can reselect rows when returning rows directly is unavailable. Prefer a stable primary-key condition; see [mutations](/mutations/).

Test schema changes against the MySQL or MariaDB version you deploy. The shared API does not make their DDL capabilities identical.

## Diagnostics and cleanup

Attach `events.onExecuteStatement` to the adaptor for execution diagnostics. Call `await driver.end()` during shutdown after pending work finishes. See [configuration](/configuration/) for lazy initialization and events.

## MariaDB nested collections

For MariaDB 10.5 or newer, use `MariaDbAdaptor` from `zodbase/adaptors/mariadb`
with the same `mysql2/promise` driver configuration. It inherits the MySQL schema,
mutation and transaction support but emits MariaDB's ordered `JSON_ARRAYAGG`
syntax for `include()` queries. MySQL requires 8.0.14 or newer for correlated
nested collections. The older `MysqlAdaptor` remains usable for MariaDB operations
that do not use `include()`.
