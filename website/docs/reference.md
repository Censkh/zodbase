---
title: API at a glance
description: A hand-written reference for Zodbase table, query, mutation, transaction, and statement APIs.
---

This reference summarizes the current API. Zodbase is under active development; there is no stable public API guarantee yet.

## Tables

| API | Result |
| --- | --- |
| `createTable({ id, schema })` | A typed table with `$field` bindings. |
| `table.addIndex(name, fields, options?)` | Registers an index on the table definition. |
| `db.syncTable(table)` | Creates or applies supported changes to the stored table. |

## Reads

| API | Behavior |
| --- | --- |
| `db.select(table)` | A lazy select builder returning all columns. |
| `db.select(table, projection)` | A lazy select builder returning named projected fields. |
| `.where(condition)` | Adds an AND condition to a select query. |
| `.orderBy(field, "ASC" \| "DESC")` | Appends an ordering term. |
| `.limit(n)`, `.offset(n)` | Sets pagination bounds. |
| `.one()` | Sets a limit of one; read the result's `first` field. |
| `.clone()` | Copies a select builder for independent changes. |
| `.forceIndex(name)` | Looks rows up through one of the table's indexes, never by scanning a whole index, on CockroachDB; ignored elsewhere. |
| `.count({ upTo? })` | The number of matching rows; with `upTo`, reads at most that many. |

## Writes

Every write is a builder: awaiting it runs it.

| API | Behavior |
| --- | --- |
| `db.insert(table, row \| rows)` | Parses and inserts one record or several; `.rows` gives the parsed rows. |
| `.onConflict(key).doNothing()` / `.doUpdate()` | Skips rows that conflict on `key`, or updates the existing rows to them. |
| `db.insert(table).fromSelect(select)` | Inserts what a select gives; its projection names the columns. |
| `db.update(table, patch).where(condition)` | Updates supplied fields; values can be expressions such as `add(field, 1)`. |
| `db.updateEach(table, patches, { by })` | Matches each patch using a field. |
| `db.delete(table).where(condition)` | Deletes only with an explicit condition. |
| `.returning(...keys)` | Runs the write, returning the stored rows (or just `keys`). |
| `db.batch([...writes])` | Runs the writes atomically, in one round trip on PostgreSQL and CockroachDB. |

## Other operations

| API | Guide |
| --- | --- |
| `db.transaction(async tx => ...)` | [Transactions](/transactions/) |
| `sql`, `raw`, `join`, `TO_SQL_SYMBOL`, `db.execute`, `db.compile` | [SQL helpers](/sql/) |
| `withCtes`, `union`, `exists`, `scalar` | [Insert, update, and delete](/mutations/#compose-writes-into-one-statement) |
| `rsqlToCondition(table, filter)` | [RSQL filters](/rsql/) |
| `primaryKey`, `foreignKey`, `backfill` | [Tables and schemas](/tables-and-schemas/) |
| `updatedAt`, `monotonicTimestamp` | [Runtime and configuration](/configuration/) |

The [source exports](https://github.com/Censkh/zodbase/blob/master/src/index.ts) and [contract tests](https://github.com/Censkh/zodbase/tree/master/tests) are the detailed implementation reference. No TypeDoc is used on this site.
