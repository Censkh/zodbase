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
| `db.select(table, ["*"])` | A lazy select builder. |
| `.where(condition)` | Adds an AND condition to a select query. |
| `.orderBy(field, "ASC" \| "DESC")` | Appends an ordering term. |
| `.limit(n)`, `.offset(n)` | Sets pagination bounds. |
| `.one()` | Sets a limit of one; read the result's `first` field. |
| `.fields(...keys)` | Changes the selected fields. |
| `.clone()` | Copies a select builder for independent changes. |
| `db.count(table).where(condition)` | Counts matching rows; `first._count` holds the count. |

## Writes

| API | Behavior |
| --- | --- |
| `db.insert(table, values)` | Parses and inserts one record. |
| `db.insertMany(table, values)` | Parses and inserts multiple records. |
| `db.update(table, patch, condition)` | Updates supplied fields. |
| `db.updateMany(table, patches, key)` | Matches each patch using a field. |
| `db.upsert(table, values, key)` | Inserts or updates by a conflict field. |
| `db.delete(table).where(condition)` | Deletes only with an explicit condition. |
| `.selectParsed()` | Insert helper returning the parsed input after writing. |
| `.selectMutated(...keys)` | Mutation helper returning stored rows; accepted fields vary by operation. |

## Other operations

| API | Guide |
| --- | --- |
| `db.transaction(async tx => ...)` | [Transactions](/transactions/) |
| `sql`, `raw`, `join`, `TO_SQL_SYMBOL`, `db.execute` | [SQL helpers](/sql/) |
| `rsqlToCondition(table, filter)` | [RSQL filters](/rsql/) |
| `primaryKey`, `foreignKey`, `backfill` | [Tables and schemas](/tables-and-schemas/) |
| `updatedAt`, `monotonicTimestamp` | [Runtime and configuration](/configuration/) |

The [source exports](https://github.com/Censkh/zodbase/blob/master/src/index.ts) and [contract tests](https://github.com/Censkh/zodbase/tree/master/tests) are the detailed implementation reference. No TypeDoc is used on this site.
