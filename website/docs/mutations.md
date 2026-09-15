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
