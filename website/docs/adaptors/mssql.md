---
title: SQL Server / Azure SQL
description: Connect to SQL Server with node-mssql, with native OUTPUT and pooled transactions.
---

## Status

This is a new adaptor. Compiler, transaction-driver and type contracts are tested locally. The SQL Server 2022 integration job in `.github/workflows/test-mssql.yml` runs on native x86 Linux; treat the adaptor as experimental until that job passes. SQL Server's x86 image crashes under QEMU on the ARM development host, so a skipped local integration suite is not a server-validation result.

## Connect

```bash
npm install zodbase zod mssql
npm install --save-dev @types/mssql
```

```ts
import { ConnectionPool } from "mssql";
import { Database } from "zodbase";
import MssqlAdaptor from "zodbase/adaptors/mssql";

const driver = await new ConnectionPool(process.env.DATABASE_URL!).connect();
const db = new Database({ adaptor: new MssqlAdaptor({ driver }) });
// On application shutdown:
// await driver.close();
```

Use a SQL Server connection string or a `node-mssql` configuration object. The application configures credentials, TLS and pool size. Azure SQL and SQL Server hosted in RDS or Cloud SQL use the same adaptor and driver; hosted authentication and networking still need provider configuration.

## Queries and transactions

- Pagination uses `TOP` or `OFFSET … FETCH`. Specify `orderBy` for deterministic pages.
- Inserts and updates return database values with `OUTPUT INSERTED`, including scalar subquery inserts. Returned bulk rows have no guaranteed order. Tables with enabled triggers can require custom SQL instead of direct `OUTPUT`.
- Strings use Unicode literals. Booleans use `BIT`; dates use `DATETIME2(3)` and round-trip in UTC; bigint reads preserve precision.
- String columns use `NVARCHAR(450)`, structured values use `NVARCHAR(MAX)`, fractional numbers use `FLOAT(53)`.
- JSON array membership uses `OPENJSON` (SQL Server 2016+, compatibility level 130+) and supports scalar elements, not nested object containment.
- Transactions acquire a native `node-mssql` transaction connection. Queries use that connection until commit/rollback. Nested transactions are rejected.
- Upserts use `MERGE … WITH (HOLDLOCK)` for key-range protection. The conflict field should have a unique constraint or primary key.
- A single `INSERT … VALUES` supports up to 1,000 rows, matching SQL Server's native limit. Split larger batches inside a transaction.

Schema synchronization supports table creation, indexes, foreign keys, primary keys, column additions/removals, nullability and explicit backfills. Schema changes run transactionally. Existing unmanaged constraints/indexes may prevent a column drop: remove those dependencies explicitly rather than expecting automatic destructive changes. SQL Server maps foreign-key `restrict` to `NO ACTION`.

## Run server integration tests

Provide `MSSQL_TEST_URL` pointing to a disposable SQL Server 2022 instance with permission to create databases, then run:

```bash
bun test tests/mssql.integration.test.ts --timeout 60000
bun test --preload zod/compile tests/mssql.integration.test.ts --timeout 60000
```

Each run creates and drops a uniquely named database. Without that environment variable the integration tests are visibly skipped; compiler and driver tests still run.

References: [node-mssql transactions](https://tediousjs.github.io/node-mssql/#transactions), [Microsoft OUTPUT semantics](https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql), [table value constructors](https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql).
