---
title: Tables and schemas
description: Define typed tables with Zod schemas and metadata, and understand Zodbase schema synchronization.
---

## Table definitions

Use `createTable({ id, schema })` to give a Zod object a database table name. Zodbase derives field bindings and input types from the schema.

```ts
import { z } from "zod";
import { createTable, metaStore, primaryKey } from "zodbase";

const Projects = createTable({
  id: "projects",
  schema: z.object({
    id: z.string().meta(metaStore([primaryKey()])),
    name: z.string(),
    description: z.string().nullable(),
  }),
});
```

Use `metaStore` to attach database metadata such as `primaryKey()` to a field. Zod validation and SQL constraints serve different purposes; describe both explicitly in your schema.

## Synchronize a schema

```ts
await db.syncTable(Projects);
```

Synchronization is repeatable. When an existing column gains `primaryKey()` metadata, it adds the missing primary key and preserves existing rows. Duplicate values are rejected rather than deleted or rewritten.

PostgreSQL and MySQL-compatible adaptors add a primary-key constraint. SQLite-compatible adaptors rebuild the table. CockroachDB replaces its implicit primary key. Replacing a different explicit primary key is not supported by automatic synchronization.

## Add required fields

Existing rows need a value when you add a required field. Attach a `backfill` value for that migration:

```ts
import { backfill, metaStore } from "zodbase";

const role = z.string().meta(metaStore([backfill({ value: "user" })]));
```

Nullable columns also require a backfill before becoming required when null values exist. Review schema changes against your adaptor and existing data before applying them.

## Input and output types

```ts
import type { InputOfTable } from "zodbase";

type NewProject = InputOfTable<typeof Projects>;
type Project = z.infer<typeof Projects.schema>;
```

`NewProject` describes what the schema accepts, while `Project` describes its parsed output. A default may allow a field to be omitted on input; a transform may change its output type. Keep transforms compatible with the stored column type.

## Column values

| Zod schema | Intent |
| --- | --- |
| `z.string()` | Text, including opaque JSON-looking strings. |
| `z.int()` / `z.number()` | Integer / numeric data; the adaptor chooses the SQL type. |
| `z.boolean()` | A boolean value with adaptor-specific storage and decoding. |
| `z.date()` | Date values stored/read with UTC millisecond precision. |
| `z.bigint()` | Integer values outside ordinary JavaScript number precision. |
| `z.object(...)` / `z.array(...)` | Structured JSON values. |
| `.nullable()` / `.optional()` | Nullable or optional fields; consider existing-row backfills. |

Use explicit schemas for structured data. Raw driver results are different from typed table reads; see [results and validation](/results-and-validation/).

## What synchronization does not replace

`syncTable` is a schema synchronization operation, not a versioned migration-history system. It compares the current table definition with the database and applies supported differences. Treat renames, destructive changes, data transformations, and deployments with old and new application versions as explicit migration work.

SQLite-compatible adaptors may rebuild tables for constraints or column changes. PostgreSQL, CockroachDB, and MySQL/MariaDB use their own schema operations. A successful change on one adaptor is not proof that an equivalent production migration is safe on another. Keep a backup and test the concrete change on representative data.

For index declarations and reference constraints, continue to [indexes and foreign keys](/indexes-and-relations/).
