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

The conflict target is a field backed by a primary key or unique constraint, or the fields of a composite key in order (`onConflict([Users.$region, Users.$id])`). `doUpdate()` sets every column the row gives but the key's to the inserted row's value. MySQL and MariaDB skip conflicts with `INSERT IGNORE`; SQL Server can't skip them.

Give `doUpdate` values to set only those. In them, `incoming(field)` is the value the inserted row brings, so the existing row can be combined with it:

```ts
import { add, incoming } from "zodbase";

// Add each day's count to what is stored for it.
await db
  .insert(DailyTotals, rows)
  .onConflict([DailyTotals.$day, DailyTotals.$metric])
  .doUpdate({ total: add(DailyTotals.$total, incoming(DailyTotals.$total)) });
```

Rows with the same key in one insert keep the last. When the key is a partial unique index, name it by its condition: `onConflict(Users.$email, { where: Users.$deletedAt.equals(null) })`. PostgreSQL, CockroachDB and SQLite take it; MySQL and MariaDB match any unique key and refuse a condition, as SQL Server does.

## Insert only where a condition holds

```ts
// Record the reply only while its ticket is still open.
await db
  .insert(Replies, reply)
  .where(exists(db.select(Tickets, { id: Tickets.$id }).where(Tickets.$id.equals(reply.ticketId).and(Tickets.$open.equals(true)))));
```

This sends `INSERT … SELECT <values> WHERE <condition>`, each value cast to its column's type, so the check and the write are one statement. The condition stands on its own, such as `exists(…)`: no table is in scope for it. It takes `onConflict` and `returning` as any insert does, and its values may include scalar subqueries; a subquery insert with `onConflict` is sent the same way.

## Insert what a select gives

```ts
await db.insert(Archive).select(
  db.select(Users, { id: Users.$id, name: Users.$name }).where(Users.$active.equals(false)),
);
```

The select's projection names the destination columns, in order. The rows never reach JavaScript. Conflicts are handled as an insert of values handles them, `.onConflict(key).doNothing()` or `.doUpdate(…)`, except on SQL Server.

## Update selected fields

```ts
await db.update(Users, { name: "Ada Lovelace" }).where(Users.$id.equals("ada"));
```

Omitted fields stay unchanged, including defaults and generated IDs. An explicit `null` is preserved if the schema permits it. An empty update object is rejected. Repeated `.where(...)` calls must all hold, as on a select. Running an update without a condition is rejected.

A field marked `updatedAt()` is stamped with the current time on every update unless the update sets it itself; set it to its own column, `{ updatedAt: Users.$updatedAt }`, to leave it as it is. Any update value may name a column this way.

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

To update a record only where a condition also holds, add `.where(...)`. In it, `incoming(field)` is the value the record brings for that field, so a stored value can be compared with it. This moves each key's `lastUsedAt` forward, never back:

```ts
import { incoming } from "zodbase";

await db
  .update(ApiKeys, touches)
  .by(ApiKeys.$id)
  .where(ApiKeys.$lastUsedAt.equals(null).or(ApiKeys.$lastUsedAt.lessThan(incoming(ApiKeys.$lastUsedAt))));
```

Every record must give each field the condition compares with `incoming(...)`. An update with a condition can't use `.returning()`, since the records it skipped can't be told from those it changed. `incoming(...)` has no value anywhere else, and a query using it is rejected.

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

Every builder (select, insert, update, delete) is a statement wherever one is taken: `db.with`, `union`, `unionAll`, `exists`, `in(...)`, `scalar` and `db.execute`.

`db.with(table, query)` gives a database whose selects and writes start with `WITH table AS (query)`, reading the query's rows through `table`, a `createTable` of its shape that is never synced. Chain `.with(…)` for several, and use `db.withRecursive` for a query that reads its own table, such as a union of a seed and a step. Selects of it are typed and parsed as any other.

On PostgreSQL and CockroachDB, the query may be an insert, update or delete with `.returning(...)`, and the statement after it sees the rows it returned, all in one statement:

```ts
const Deactivated = createTable({ id: "deactivated", schema: Users.schema });
await db
  .with(Deactivated, db.update(Users, { active: false }).where(Users.$lastSeen.lessThan(cutoff)).returning())
  .insert(AuditLog)
  .select(db.select(Deactivated, { userId: Deactivated.$id, action: expression(sql`'deactivated'`, z.string()) }));
```

Elsewhere a WITH clause holds selects only. SQLite runs a select, insert, update or delete after one; MySQL and MariaDB run only selects after one. A delete returns the rows it removed with `.returning(...)` on PostgreSQL, CockroachDB and SQLite. MySQL and MariaDB refuse an update or delete whose condition reads the table it writes ("can't specify target table"): read the rows' keys first, then write by them.

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

Use `.returning()` for computed values; an insert of subquery values has no parsed `.rows`. PostgreSQL, CockroachDB and SQLite-family adaptors return values using the same statement's `RETURNING`. The MySQL adaptor (including MariaDB) supports awaiting subquery inserts but rejects `.returning()` before writing. D1 write-only transactions support plain subquery inserts in their atomic batch, but cannot return rows from inside the transaction.

An async function must not return a SELECT builder directly: JavaScript awaits thenables automatically. Return `{ homeRegion: query }` if a helper needs to return an unevaluated subquery.

### Adaptor query plans

Scalar inserts project the stored column directly, without read-decoding casts. Equality predicates retain the indexed column, and the builder adds no implicit ORDER BY or LIMIT. `RETURNING` is emitted only when requested and supported.

PostgreSQL shares repeated deterministic primary-key lookups through a materialized CTE within the INSERT. Different keys, self-reads, arbitrary SQL expressions and ordered searches remain independent. Cockroach keeps inline subqueries: materialized buffers disable its auto-commit fast path. MySQL/MariaDB retain native constant-key subqueries; SQLite/Turso retain indexed scalar lookups without temporary materialization. Bulk inserts still use one statement.

Regression tests inspect real EXPLAIN plans on all six engines, including Cockroach REGIONAL BY ROW tables. These verify plan shape and index use; they do not measure production cross-region network latency.
