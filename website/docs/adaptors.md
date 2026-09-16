---
title: Adaptors
description: Choose a Zodbase database adaptor and find its installation, connection, transaction, and schema guide.
---

An adaptor translates Zodbase operations for a database driver. Choose your runtime below for setup, transaction behavior, schema notes, and connection cleanup.

| Adaptor | Driver | Transaction behavior |
| --- | --- | --- |
| [Bun SQLite](/adaptors/bun-sqlite/) | Built-in `bun:sqlite` | SQL transaction on one connection |
| [better-sqlite3](/adaptors/better-sqlite3/) | `better-sqlite3` | SQL transaction on one connection |
| [Cloudflare D1](/adaptors/d1/) | Worker D1 binding | Write-only batch |
| [PostgreSQL](/adaptors/postgres/) | `pg` | Dedicated client for pooled transactions |
| [Neon HTTP](/adaptors/neon-http/) | `@neondatabase/serverless` | Single-request queries; interactive transactions rejected |
| [SQL Server / Azure SQL](/adaptors/mssql/) (experimental) | `mssql` | Native pooled transaction connection |
| [MySQL / MariaDB](/adaptors/mysql/) | `mysql2/promise` | Dedicated connection for pooled transactions |
| [CockroachDB](/adaptors/cockroach/) | `pg` | Retries serialization failures |
| [Turso / libSQL](/adaptors/turso/) | `@libsql/client` | libSQL write transaction |
| [Expo SQLite](/adaptors/expo-sqlite/) | `expo-sqlite` | Explicit SQL transaction |

The [shared SQLite base](/adaptors/sqlite/) documents common schema behavior and the extension point for other drivers. It is abstract, not a concrete connection adaptor.

## Shared setup

Import the adaptor from `zodbase/adaptors/<name>` and construct it with `{ driver }`. The application owns the driver's lifetime. Credentials, TLS, pool limits, and driver options belong in driver configuration.

You can pass an async initializer to `Database` to defer connection setup. It runs at most once per database instance; see [runtime and configuration](/configuration/).

## Shared API, different capabilities

The query API is shared, but transaction capabilities, returning rows, and schema changes depend on the adaptor. Start with its individual guide, then read [transactions](/transactions/), [mutations](/mutations/), and [tables and schemas](/tables-and-schemas/) for the common contracts.

See the [cloud provider mapping](/adaptors/cloud-providers/) for Supabase, AWS, Google Cloud and Azure, and the [subquery test contracts](/subquery-testing/) for validation scope.
