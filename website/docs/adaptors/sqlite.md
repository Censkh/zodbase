---
title: Shared SQLite base
description: Understand the abstract SQLite adaptor behind Bun, better-sqlite3, D1, Expo, and Turso integrations.
---

`zodbase/adaptors/sqlite` exports an **abstract base class**, not a standalone database driver. Choose [Bun SQLite](/adaptors/bun-sqlite/), [better-sqlite3](/adaptors/better-sqlite3/), [D1](/adaptors/d1/), [Expo SQLite](/adaptors/expo-sqlite/), or [Turso](/adaptors/turso/) for an application connection.

## Shared behavior

The base implements SQLite query generation, schema inspection, indexes, and supported schema changes. JSON array membership uses `json_each`. Concrete adaptors supply statement execution and driver-specific bulk updates or transaction handling.

## Schema synchronization

`syncTable` creates missing tables and applies supported differences. Changes that cannot be expressed directly can rebuild a table and copy its rows. Adding a required field to populated data, or making a nullable field required when nulls remain, requires a non-null backfill.

Adding a primary key preserves existing rows when their values satisfy the constraint. Duplicate values fail instead of being silently removed. Read [tables and schemas](/tables-and-schemas/) for the backfill contract and migration boundaries.

## Foreign keys

Table creation issues `PRAGMA foreign_keys = ON`. Foreign-key enforcement is a connection concern: ensure it is enabled on connections that access existing tables too. See [indexes and foreign keys](/indexes-and-relations/).

## Transactions differ by driver

Bun, better-sqlite3, and Expo inherit the base SQL transaction path. D1 overrides it with write-only batches; Turso uses a libSQL write transaction. Consult the concrete adaptor's guide before relying on reads or returning helpers inside a transaction.

## Implementing another SQLite driver

Subclass the base and supply `execute(statement)` and `executeUpdateMany(...)`. Normalize results to `{ results, first }`, and override transaction execution if the driver cannot use the default SQL `BEGIN`/`COMMIT`/`ROLLBACK` path. The existing concrete adaptors are the implementation examples.
