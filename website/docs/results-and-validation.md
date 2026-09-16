---
title: Results and validation
description: Understand Zodbase input parsing, defaults, row decoding, mutation returning helpers, and lazy query execution.
---

## The result object

```ts
const result = await db.select(Users, { id: Users.$id, name: Users.$name });
console.log(result.results); // Selected records
console.log(result.first);   // First record, or undefined
```

`results` is the array of rows. `first` does not throw for an empty query. Some adaptors also report `timings`; do not assume optional timing or pagination fields are always present.

## Validation happens on writes

`insert`, `insertMany`, and `upsert` parse complete records through the Zod table schema. `update` and `updateMany` validate the supplied fields while preserving omitted fields. Put both construction and awaiting of an operation inside your error-handling block when you need to catch validation and driver failures.

```ts
try {
  await db.insert(Users, input);
} catch (error) {
  // Distinguish Zod validation failures from driver errors in your application.
}
```

Defaults and transforms can make the accepted input different from the stored value type. Use `InputOfTable<typeof Users>` for insert inputs; use a schema's inferred output type for parsed values.

## Parsed input versus stored rows

| Execution path | Returned data |
| --- | --- |
| `await db.insert(...)` | No rows: empty `results`, undefined `first`. |
| `await db.insert(...).selectParsed()` | The Zod-parsed input after insertion. |
| `await db.insert(...).selectMutated()` | Rows read back from the database. |
| `await db.update(...).selectMutated("id")` | Selected fields from the updated records. |

Parsed input cannot include database-generated changes that Zod did not produce. Returned rows may use native `RETURNING` or an adaptor-specific follow-up query. If a fallback must reselect by the original condition, changing a field in that condition can affect which rows are found. Prefer a stable primary-key condition when requesting updated rows.

## Typed reads and raw reads

Typed table reads decode column values using their schemas. Explicit object and array schemas describe structured JSON; string schemas preserve opaque text. Dates are handled as UTC with millisecond precision by the supported adaptor contracts.

Raw `db.execute(sql\`...\`)` returns driver values. It does not infer a table schema or guess whether a string is JSON. See [SQL helpers](/sql/) when you need a driver-level statement.

## Lazy execution

Query builders execute on `await`, `.then`, `.catch`, or `.finally`. The promise execution is memoized: awaiting the same builder again reuses the result. To run a fresh query, construct a new builder or clone it before execution. Returning helpers are separate execution methods, so choose one helper or plain awaiting for each mutation.
