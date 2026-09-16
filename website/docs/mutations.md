---
title: Insert, update, and delete
description: Write records with Zodbase, apply partial updates, perform bulk operations and upserts, and request returned rows.
---

These examples use `db` and `Users` from [getting started](/). Write operations validate against the table schema. Await the operation to execute it.

## Insert one record

```ts
await db.insert(Users, { id: "ada", name: "Ada", active: true });
```

The input passes through `schema.parse`: declared defaults and transforms are applied. Validation errors can be thrown when constructing the operation, before a query reaches the driver.

## Insert many records

```ts
await db.insertMany(Users, [
  { id: "grace", name: "Grace", active: true },
  { id: "margaret", name: "Margaret", active: false },
]);
```

Each row is parsed before execution. Awaiting `insertMany(Users, [])` is a no-op. Do not call its returning helpers for an empty list: the current empty-list path returns an ordinary resolved promise.

## Update selected fields

```ts
await db.update(Users, { name: "Ada Lovelace" }, Users.$id.equals("ada"));
```

The third argument is the condition. Omitted fields stay unchanged, including defaults and generated IDs. A supplied `undefined` can invoke that field's declared default; an explicit `null` is preserved if the schema permits it. An empty update object is rejected.

## Update several records by a key

```ts
await db.updateMany(Users, [
  { id: "ada", active: false },
  { id: "grace", name: "Grace Hopper" },
], Users.$id);
```

Include the matching key in every item. Each item updates only its supplied fields. Bulk updates are not a substitute for an explicit [transaction](/transactions/) when multiple writes must succeed together; adaptor implementations differ.

## Upsert

```ts
await db.upsert(Users, {
  id: "ada",
  name: "Ada Lovelace",
  active: true,
}, Users.$id);
```

The last argument identifies the conflict field. Back it with a primary key or a unique constraint supported by the database. Upsert parses a complete row, unlike a partial update.

## Delete with a condition

```ts
await db.delete(Users).where(Users.$id.equals("margaret"));
```

Deleting without a `where` condition is rejected when executed. Combine multiple conditions with `.and(...)` or `.or(...)` before passing them to the delete builder. Unlike select queries, repeated delete `.where(...)` calls replace the previous condition.

## Return the written rows

```ts
const inserted = await db.insert(Users, {
  id: "katherine", name: "Katherine", active: true,
}).selectMutated();

const updated = await db.update(
  Users, { active: false }, Users.$id.equals("katherine"),
).selectMutated("id", "active");
```

Ordinary awaited writes return `{ results: [], first: undefined }`. `.selectMutated()` asks for the stored rows; `.selectParsed()` on inserts returns the parsed input after writing it. Choose one execution path: do not await a mutation and then call a returning helper on the same object, because the helper executes the write again. See [results and validation](/results-and-validation/) for the distinction and adaptor limitations.

## Use a scalar subquery as an insert value

Pass a single-column `select` builder directly as a value in `insert` or `insertMany`. Do **not** await the nested SELECT:

```ts
const result = await db.insert(ProjectLayers, {
  id: layerId,
  projectId,
  userId,
  homeRegion: db.select(Projects, { homeRegion: Projects.$homeRegion })
    .where(Projects.$id.equals(projectId).and(Projects.$userId.equals(userId))),
}).selectMutated();
```

This sends one `INSERT ... VALUES (..., (SELECT ...))` statement. The nested query runs on the insert's connection and within its transaction; it never fetches a parent row into JavaScript. Reuse a region already loaded for authorization when available. Include the appropriate ownership condition when the subquery also needs to enforce access.

Subqueries must select exactly one column with a compatible output type. Use a unique-key condition to produce at most one row. No match produces SQL `NULL`, so a required destination column rejects the write. PostgreSQL, CockroachDB and MySQL reject multiple matches; SQLite uses the first match. Use an explicit order and `.one()` only when choosing one result is intentional.

Literal fields still receive Zod validation, defaults and transforms. Subquery fields bypass JavaScript validation/defaults/transforms and are checked by database constraints. Subquery inserts require an object schema without object-level refinements (those cannot be evaluated before the SQL result exists). The SELECT structure is captured when constructing the insert.

Use `selectMutated()` for computed values; `selectParsed()` is unavailable on subquery inserts. PostgreSQL, CockroachDB and SQLite-family adaptors return values using the same statement's `RETURNING`. The MySQL adaptor (including MariaDB) supports awaiting subquery inserts but rejects `selectMutated()` before writing. D1 write-only transactions support plain subquery inserts in their atomic batch, but cannot return rows from inside the transaction.

An async function must not return a SELECT builder directly: JavaScript awaits thenables automatically. Return `{ homeRegion: query }` if a helper needs to return an unevaluated subquery.

### Adaptor query plans

Scalar inserts project the stored column directly, without read-decoding casts. Equality predicates retain the indexed column, and the builder adds no implicit ORDER BY or LIMIT. `RETURNING` is emitted only when requested and supported.

PostgreSQL shares repeated deterministic primary-key lookups through a materialized CTE within the INSERT. Different keys, self-reads, arbitrary SQL expressions and ordered searches remain independent. Cockroach keeps inline subqueries: materialized buffers disable its auto-commit fast path. MySQL/MariaDB retain native constant-key subqueries; SQLite/Turso retain indexed scalar lookups without temporary materialization. Bulk inserts still use one statement.

Regression tests inspect real EXPLAIN plans on all six engines, including Cockroach REGIONAL BY ROW tables. These verify plan shape and index use; they do not measure production cross-region network latency.
