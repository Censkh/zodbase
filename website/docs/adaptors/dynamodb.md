---
title: Amazon DynamoDB
description: Store Zodbase tables in DynamoDB, with keys and global secondary indexes taken from your schema.
---

```bash
npm install zodbase zod @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
```

```ts
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { z } from "zod";
import { createTable, Database, metaStore, primaryKey } from "zodbase";
import DynamoDbAdaptor from "zodbase/adaptors/dynamodb";

const Letters = createTable({
  id: "letters",
  schema: z.object({
    id: z.string().meta(metaStore([primaryKey()])),
    pairId: z.string(),
    createdAt: z.number(),
    body: z.string(),
  }),
});
Letters.addIndex("letters_by_pair", [Letters.$pairId, Letters.$createdAt]);

const driver = new DynamoDBClient({ region: "eu-west-2" });
const db = new Database({ adaptor: new DynamoDbAdaptor({ driver, tablePrefix: "app-main-" }) });

await db.syncTable(Letters);
const newest = await db
  .select(Letters)
  .where(Letters.$pairId.equals("pair-1"))
  .orderBy(Letters.$createdAt, "DESC")
  .limit(20);
```

DynamoDB is not a relational SQL engine, so this adaptor supports a subset of Zodbase, chosen so that every supported query stays cheap. It is tested against [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html), not a hosted AWS account.

## Keys and indexes

- **Each table is a DynamoDB table.** `tablePrefix` is prepended to its id. It is created with on-demand (pay-per-request) billing.
- **The `primaryKey()` field is the partition key.** A table needs exactly one, and it must be a string, number or date.
- **`table.addIndex(id, [partition, sort?])` is a global secondary index**, with every attribute projected. Its first field is the index's partition key; an optional second field is its sort key. Unique and partial indexes are refused, because DynamoDB cannot enforce them.
- **`syncTable` creates the table and any missing indexes**, one index at a time, waiting until each is active. As with the SQL adaptors, it never drops an index. DynamoDB keeps no columns, so adding, removing or changing fields needs no migration.
- **An index is sparse:** a row whose index key is `null` is left out of that index, as DynamoDB requires.

## How a select is answered

The adaptor answers each select with the cheapest call that can:

1. **`id.equals(…)` on the primary key:** a strongly consistent `GetItem`.
2. **`id.in([...])` on the primary key:** `BatchGetItem`.
3. **`equals(…)` on an index's partition key:** a `Query` on that index. A range condition (`equals`, `lessThan`, `greaterThan` and their `OrEquals` forms) on its sort key narrows it, and ordering by its sort key reads in order. When several indexes fit, the one that sorts the way the query asks wins.
4. **Anything else:** a full-table `Scan`, refused with `dynamodb/needs-index` unless the adaptor is created with `allowScans: true`.

Conditions the key cannot answer, including `like`, `or`, `contains`, `notIn` and comparisons between fields, filter the rows read, with SQL's `NULL` semantics. Ordering by another field, `offset` and `limit` apply after the read. A select reads every item its key matches before filtering, so give large partitions a sort key and a range condition.

Index queries are **eventually consistent**: DynamoDB updates global secondary indexes asynchronously, so a row written a moment ago may not yet appear when read through an index. Reads by primary key are strongly consistent.

## Mutations

| Call | DynamoDB |
| --- | --- |
| `insert` | `PutItem` that refuses an existing key (`dynamodb/duplicate-key`) |
| `insert` of several rows | `TransactWriteItems`: each batch of up to 100 rows is all or nothing |
| `update`, `updateEach` | Reads the matching rows, then one `UpdateItem` each. A row changed underneath a `monotonicTimestamp` update is read again and retried |
| `.onConflict(key).doUpdate(values?)` | Reads the row `key` names (by the primary key, or an index on the key's fields), then updates it, or inserts the row if there is none. `incoming(field)` in `values` is the inserted row's value |
| `.onConflict(key).doNothing()` | Reads the row `key` names, and inserts the row only if there is none |
| `delete` | Reads the matching rows, then `BatchWriteItem` deletes |
| `select(…).count({ upTo? })` | Counts the rows the same read returns |

`returning()` gives the rows as DynamoDB stored them.

## Values

Strings, numbers, booleans, objects and arrays are stored natively. Dates are stored as ISO 8601 strings and `bigint` values as decimal strings; both are decoded back on read. A field with no stored attribute reads as `null`.

## Not supported

`db.execute` (raw SQL), joins, `include()`, projections, inserts with subqueries or from a select, computed values (`add(…)`, a column as a value), a conflict target's `where`, `db.batch` and `db.transaction` (DynamoDB transactions are a single request; insert several rows at once instead) are refused with `dynamodb/unsupported` before anything is sent.

Errors are `ZodbaseDynamoDbError`, with a `code` of `dynamodb/unsupported`, `dynamodb/needs-index`, `dynamodb/duplicate-key` or `dynamodb/invalid-key`.
