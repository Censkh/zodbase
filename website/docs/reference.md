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
| `.groupBy(...fields)` | One row per group, for aggregates such as `sum`. |
| `greatest(a, b)` | The larger of two numbers, such as a total kept at zero or more. |
| `.crossJoin(table)` | Pairs every row with every row of `table`. |
| `values(table, rows)` | Literal rows read as a table under `table`'s name. |
| `exists(query)` / `notExists(query)` | Whether a (correlated) query gives any row. |

## Writes

Every write is a builder: awaiting it runs it.

| API | Behavior |
| --- | --- |
| `db.insert(table, row \| rows)` | Parses and inserts one record or several; `.rows` gives the parsed rows. |
| `.onConflict(key, { where? }).doNothing()` / `.doUpdate(values?)` | Skips rows that conflict on `key` (a partial unique index by its `where`), or updates the existing rows: to `values`, where `incoming(field)` is the inserted value, or to every column the row gives. |
| `.where(condition)` on an insert | Inserts the rows only where a standalone condition, such as `exists(…)`, holds. |
| `db.insert(table).select(query)` | Inserts what a select gives; its projection names the columns. Takes `onConflict`. |
| `db.update(table, patch).where(condition)` | Updates supplied fields; values can be expressions such as `add(field, 1)`. |
| `db.update(table, records).by(key)` | Updates each record found by a key field, or a compound key's fields. |
| `db.update(table, records).by(key).where(condition)` | Updates each record only where the condition holds; `incoming(field)` is the record's value for a field. |
| `db.delete(table).where(condition)` | Deletes only with an explicit condition. |
| `.returning(...keys)` on a delete | The rows it removed, as one statement on PostgreSQL and CockroachDB. |
| `.returning(...keys)` | Runs the write, returning the stored rows (or just `keys`). |
| `db.batch([...writes])` | Runs the writes atomically, in one round trip on PostgreSQL and CockroachDB. |
| `db.load(field, value, { where? })` | The row whose unique `field` is `value`; loads started in the same tick share one `IN (…)` query. |

## Other operations

| API | Guide |
| --- | --- |
| `db.transaction(async tx => ...)` | [Transactions](/transactions/) |
| `sql`, `raw`, `join`, `TO_SQL_SYMBOL`, `db.execute`, `db.compile` | [SQL helpers](/sql/) |
| `db.with`, `db.withRecursive`, `union`, `unionAll`, `exists`, `scalar` | [Insert, update, and delete](/mutations/#compose-writes-into-one-statement) |
| `rsqlToCondition(table, filter)` | [RSQL filters](/rsql/) |
| `primaryKey`, `foreignKey`, `backfill` | [Tables and schemas](/tables-and-schemas/) |
| `updatedAt`, `monotonicTimestamp` | [Runtime and configuration](/configuration/) |

The [source exports](https://github.com/Censkh/zodbase/blob/master/src/index.ts) and [contract tests](https://github.com/Censkh/zodbase/tree/master/tests) are the detailed implementation reference. No TypeDoc is used on this site.
