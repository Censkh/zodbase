---
title: Insert, update, and delete
description: Write records with Zodbase, apply partial updates, handle conflicts, return written rows, and run several writes atomically.
---

These examples use `db` and `Users` from [getting started](/). Every write is a builder: chain what it needs, then await it to run it. Write operations validate against the table schema.

## Insert one record or several

```ts
await db.insert(Users, { id: "ada", name: "Ada", active: true });

await db.insert(Users, [
  { id: "grace", name: "Grace", active: true },
  { id: "margaret", name: "Margaret", active: false },
]);
```

Each row passes through `schema.parse`: declared defaults and transforms are applied. Validation errors are thrown when constructing the insert, before a query reaches the driver. Inserting an empty list is a no-op.

An insert's parsed rows are available without a round trip, as `.rows`:

```ts
const insert = db.insert(Users, { id: "ada", name: "Ada" });
await insert;
insert.rows[0]; // { id: "ada", name: "Ada", active: true }, with the schema's defaults
```

## Handle conflicting rows

```ts
// Skip rows whose key is already taken.
await db.insert(Users, rows).onConflict(Users.$id).doNothing();

// Update the existing row to the inserted values (an upsert).
await db.insert(Users, { id: "ada", name: "Ada Lovelace", active: true }).onConflict(Users.$id).doUpdate();
```

The conflict target is a field backed by a primary key or unique constraint, or the fields of a composite key in order (`onConflict([Users.$region, Users.$id])`). `doUpdate` sets every column to the inserted row's value. MySQL and MariaDB skip conflicts with `INSERT IGNORE`; SQL Server can't skip them.

## Insert what a select gives

```ts
await db.insert(Archive).fromSelect(
  db.select(Users, { id: Users.$id, name: Users.$name }).where(Users.$active.equals(false)),
);
```

The select's projection names the destination columns, in order. The rows never reach JavaScript.

## Update selected fields

```ts
await db.update(Users, { name: "Ada Lovelace" }).where(Users.$id.equals("ada"));
```

Omitted fields stay unchanged, including defaults and generated IDs. An explicit `null` is preserved if the schema permits it. An empty update object is rejected. Repeated `.where(...)` calls must all hold, as on a select. Running an update without a condition is rejected.

Values can be expressions of the row's own columns, such as a counter's next value:

```ts
await db.update(Projects, { assetCount: add(Projects.$assetCount, 1) }).where(Projects.$id.equals(id));
```

## Update several records by a key

Give `update` an array of records and name the key that finds each one with `.by(...)`:

```ts
await db
  .update(Users, [
    { id: "ada", active: false },
    { id: "grace", name: "Grace Hopper" },
  ])
  .by(Users.$id);
```

Each record updates only the fields it supplies, and must include every key field. A record whose key matches no row changes nothing; a key given more than once keeps its last record. For a compound key, pass its fields in order:

```ts
await db.update(Placements, rows).by([Placements.$region, Placements.$id]);
```

On PostgreSQL and CockroachDB the records go as one statement per set of fields they change, joined to the table on the key, so the database writes them together rather than one round trip at a time. Other adaptors run one update per record.

## Delete with a condition

```ts
await db.delete(Users).where(Users.$id.equals("margaret"));
```

Deleting without a `where` condition is rejected when executed. Repeated `.where(...)` calls must all hold.

## Return the written rows

```ts
const inserted = await db.insert(Users, { id: "katherine", name: "Katherine", active: true }).returning();

const updated = await db
  .update(Users, { active: false })
  .where(Users.$id.equals("katherine"))
  .returning("id", "active");
```

An awaited write returns `{ results: [], first: undefined }`. `.returning()` gives the stored rows, or just the fields named. With `onConflict(…).doNothing()`, it gives only the rows inserted. See [results and validation](/results-and-validation/) for adaptor limitations.

## Run several writes atomically

```ts
const [asset, events] = await db.batch([
  db.insert(Assets, asset).returning(),
  db.insert(Events, events),
  db.update(Projects, { assetCount: add(Projects.$assetCount, 1) }).where(Projects.$id.equals(projectId)),
]);
```

The writes are stored together or not at all. Each result is what awaiting that write alone gives. PostgreSQL and CockroachDB receive every statement in one round trip, which they run as one transaction; other databases run the writes in a [transaction](/transactions/). No write may depend on another's rows, because every statement sees the database as it was before the batch.

## Compose writes into one statement

Every builder (select, insert, update, delete) is a statement wherever one is taken: `withCtes`, `union`, `exists`, `in(...)`, `scalar` and `db.execute`. On PostgreSQL and CockroachDB, an update or insert with `.returning(...)` is a writable CTE:

```ts
const Updated = createTable({ id: "updated_user", schema: Users.schema });
await db.execute(
  withCtes(
    [{ table: Updated, query: db.update(Users, { active: false }).where(Users.$id.equals("ada")).returning() }],
    db.select(Updated),
  ),
);
```

## Use a scalar subquery as an insert value

Pass a single-column `select` builder directly as a value in `insert`. Do **not** await the nested SELECT:

```ts
const result = await db
  .insert(ProjectLayers, {
    id: layerId,
    projectId,
    userId,
    homeRegion: db
      .select(Projects, { homeRegion: Projects.$homeRegion })
      .where(Projects.$id.equals(projectId).and(Projects.$userId.equals(userId))),
  })
  .returning();
```

This sends one `INSERT ... VALUES (..., (SELECT ...))` statement. The nested query runs on the insert's connection and within its transaction; it never fetches a parent row into JavaScript. Reuse a region already loaded for authorization when available. Include the appropriate ownership condition when the subquery also needs to enforce access.

Subqueries must select exactly one column with a compatible output type. Use a unique-key condition to produce at most one row. No match produces SQL `NULL`, so a required destination column rejects the write. PostgreSQL, CockroachDB and MySQL reject multiple matches; SQLite uses the first match. Use an explicit order and `.one()` only when choosing one result is intentional.

Literal fields still receive Zod validation, defaults and transforms. Subquery fields bypass JavaScript validation/defaults/transforms and are checked by database constraints. Subquery inserts require an object schema without object-level refinements (those cannot be evaluated before the SQL result exists). The SELECT structure is captured when constructing the insert.

Use `.returning()` for computed values; an insert of subquery values has no parsed `.rows` and can't handle conflicts. PostgreSQL, CockroachDB and SQLite-family adaptors return values using the same statement's `RETURNING`. The MySQL adaptor (including MariaDB) supports awaiting subquery inserts but rejects `.returning()` before writing. D1 write-only transactions support plain subquery inserts in their atomic batch, but cannot return rows from inside the transaction.

An async function must not return a SELECT builder directly: JavaScript awaits thenables automatically. Return `{ homeRegion: query }` if a helper needs to return an unevaluated subquery.

### Adaptor query plans

Scalar inserts project the stored column directly, without read-decoding casts. Equality predicates retain the indexed column, and the builder adds no implicit ORDER BY or LIMIT. `RETURNING` is emitted only when requested and supported.

PostgreSQL shares repeated deterministic primary-key lookups through a materialized CTE within the INSERT. Different keys, self-reads, arbitrary SQL expressions and ordered searches remain independent. Cockroach keeps inline subqueries: materialized buffers disable its auto-commit fast path. MySQL/MariaDB retain native constant-key subqueries; SQLite/Turso retain indexed scalar lookups without temporary materialization. Bulk inserts still use one statement.

Regression tests inspect real EXPLAIN plans on all six engines, including Cockroach REGIONAL BY ROW tables. These verify plan shape and index use; they do not measure production cross-region network latency.
