---
title: Queries
description: Insert records, select typed fields, and filter Zodbase queries with table field bindings.
---

## Insert records

Use the table definition from the [getting-started guide](/):

```ts
await db.insert(Users, {
  id: "ada",
  name: "Ada",
  active: true,
});
```

## Select records

Select all fields with `"*"`, then read the result's `results` array:

```ts
const { results, first } = await db.select(Users);
```

`first` is the first returned record, or `undefined` when there is no match.

## Filter with field bindings

```ts
const query = db.select(Users)
  .where(Users.$id.equals("ada"));

const { first: user } = await query;
```

Queries execute when awaited. Keep values in typed conditions rather than constructing SQL strings from user input.

## Select specific fields

```ts
const { results } = await db.select(Users, { id: Users.$id, name: Users.$name });
```

The selected fields determine the result shape. See the [source exports](https://github.com/Censkh/zodbase/blob/master/src/index.ts) for the current query API.

## Combine conditions

```ts
const condition = Users.$active.equals(true)
  .and(Users.$name.like("A%"))
  .or(Users.$id.equals("grace"));

const { results } = await db.select(Users).where(condition);
```

A compound condition is grouped when rendered to SQL. `.and(...)` and `.or(...)` accept falsy clauses, which is useful for optional filters. Repeated select `.where(...)` calls add AND conditions:

```ts
const query = db.select(Users, { id: Users.$id, name: Users.$name })
  .where(Users.$active.equals(true));

if (searchTerm) query.where(Users.$name.like(`${searchTerm}%`));
```

SQL LIKE uses `%` for any sequence and `_` for one character. Whether comparison is case-sensitive depends on your database and collation.

## Field operators

| Method | Meaning |
| --- | --- |
| `.equals(value)` / `.notEquals(value)` | Equality / inequality; null renders as `IS NULL` / `IS NOT NULL`. |
| `.greaterThan(value)` / `.lessThan(value)` | Strict comparisons. |
| `.greaterThanOrEquals(value)` / `.lessThanOrEquals(value)` | Inclusive comparisons. |
| `.in(values)` / `.notIn(values)` | Membership in a value list. |
| `.like(pattern)` | SQL LIKE matching. |
| `.contains(value)` | JSON-array membership, using the adaptor's implementation. |

An empty `in([])` condition matches no rows. An empty `notIn([])` matches all rows. Use schema-compatible values; a nullable field is needed for a typed null equality check.

## Order and paginate

```ts
const page = await db.select(Users, { id: Users.$id, name: Users.$name })
  .orderBy(Users.$name, "ASC")
  .orderBy(Users.$id, "ASC")
  .limit(20)
  .offset(40);
```

Directions are uppercase `"ASC"` or `"DESC"`. Ordering calls append terms. Include a unique tie-breaker so records with the same name have a stable order. `.limit(0)` returns no rows. Offset pagination can shift when records are inserted or removed between requests.

For a simple keyset cursor ordered by a unique ID:

```ts
const page = await db.select(Users, { id: Users.$id, name: Users.$name })
  .where(Users.$id.greaterThan(lastSeenId))
  .orderBy(Users.$id, "ASC")
  .limit(20);
```

For multiple sort keys, build the corresponding compound continuation condition. Null ordering varies by database, so define an explicit policy for nullable cursor fields.

## Fetch one record

```ts
const { first } = await db.select(Users)
  .where(Users.$id.equals("ada"))
  .one();
```

`.one()` applies a limit of one. It does not require a match and does not assert uniqueness; `first` is still possibly undefined.

## Count records

```ts
const total = await db.count(Users).where(Users.$active.equals(true));
console.log(total.first._count);

const names = await db.count(Users, "name");
console.log(names.first.name);
```

Counting a field counts its non-null values. A select builder's `.count()` uses its selected fields and condition, without applying its order, limit, or offset. Prefer `db.count(table)` for a total row count.

## Reuse a base query

```ts
const active = db.select(Users, { id: Users.$id, name: Users.$name })
  .where(Users.$active.equals(true));

const ascending = active.clone().orderBy(Users.$name, "ASC");
const descending = active.clone().orderBy(Users.$name, "DESC");
const [a, b] = await Promise.all([ascending, descending]);
```

Build and clone queries before executing them. The builder stores query state, and its lazy execution is memoized. Do not mutate an already-awaited builder expecting a fresh database read.

## Joined and nested reads

See [Joins and includes](/joins/) for typed joins, nested projections, self joins,
and single-statement related collections. [Join test coverage](/join-testing/)
describes the cross-database contracts and upstream test-suite inspiration.
