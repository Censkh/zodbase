---
title: Transactions
description: Group Zodbase operations into transactions, handle rollback, and understand D1 and CockroachDB differences.
---

## Commit a group of operations

Use the `Database` instance passed into the callback for every operation that belongs to the transaction:

```ts
const userId = await db.transaction(async (tx) => {
  await tx.insert(Users, { id: "lin", name: "Lin", active: true });
  await tx.update(Users, { name: "Lin Chen" }, Users.$id.equals("lin"));
  return "lin";
});
```

The callback's resolved value becomes the transaction result. Await all operations before returning. Calling the outer `db` inside the callback does not route an operation through the transaction instance.

## Roll back on failure

```ts
try {
  await db.transaction(async (tx) => {
    await tx.insert(Users, { id: "temporary", name: "Temporary", active: true });
    throw new Error("Cancel this change");
  });
} catch (error) {
  // Handle the failure; the transaction's write was rolled back.
}
```

A thrown value or failed database operation causes rollback on transactional adaptors. Do not catch and suppress an error inside the callback if you need the whole transaction to roll back. Nested transactions are not supported.

## Adaptor behavior

| Adaptor | Transaction model |
| --- | --- |
| PostgreSQL / MySQL | A driver connection executes the transaction's statements. |
| Bun SQLite / better-sqlite3 | SQLite transaction statements wrap the operations. |
| Turso | A libSQL write transaction is committed or rolled back. |
| Cloudflare D1 | Writes are collected and submitted as a D1 batch. |
| CockroachDB | Serialization failures can replay the entire callback. |

### D1 batches

D1 does not expose an interactive SQL transaction through this adaptor. The callback collects writes for a batch; reads and operations that require returned rows inside the transaction are rejected. Calculate inputs before entering the transaction and use write-only operations in the callback.

### CockroachDB retries

Keep the callback safe to run again. Avoid sending email, charging a card, or publishing external events from inside a transaction callback that may be retried. Perform those actions after the transaction succeeds, or use a database-backed outbox in your application. See the [retry contract](https://github.com/Censkh/zodbase/blob/master/tests/cockroachRetry.test.ts) for the current retry behavior.
