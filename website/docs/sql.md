---
title: SQL helpers
description: Use Zodbase SQL templates, identifier bindings, raw fragments, and driver-level execution deliberately.
---

## Execute a statement

```ts
import { sql } from "zodbase";

const id = "ada";
const result = await db.execute(
  sql`SELECT * FROM ${Users} WHERE ${Users.$id} = ${id}`,
);
```

The `sql` tag returns a statement object. Interpolated ordinary values pass through Zodbase's SQL-literal escaping; table and field bindings provide quoted identifiers. This API renders SQL text—it is not a prepared-statement parameter API.

## Use trusted raw fragments

```ts
import { raw, sql } from "zodbase";

const direction = descending ? raw("DESC") : raw("ASC");
const statement = sql`SELECT * FROM ${Users} ORDER BY ${Users.$name} ${direction}`;
```

`raw` inserts text without escaping it. Use fixed, application-owned fragments such as the two values above. Never pass request text directly to `raw`. For ordinary comparisons and sorting, prefer the typed query builder so the adaptor can render the correct dialect.

## Inspect a statement

```ts
import { TO_SQL_SYMBOL } from "zodbase";

const text = statement[TO_SQL_SYMBOL]();
```

Generated SQL can contain data values. Keep sensitive statements out of public logs. Raw statements are database-specific, and raw results retain the driver's values rather than Zodbase's typed row decoding.

## Helper reference

| Helper | Purpose |
| --- | --- |
| `sql` | Template tag that renders escaped values and SQL-capable bindings. |
| `raw` | Explicitly trusted SQL text or fragments. |
| `join` | Join SQL-capable items with a separator. |
| `TO_SQL_SYMBOL` | Obtain the rendered SQL string from a statement. |
| `db.execute` | Execute a statement directly through the adaptor. |
