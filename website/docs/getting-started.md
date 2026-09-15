---
id: getting-started
slug: /
title: Your schema. Your database.
sidebar_label: Getting started
hide_title: true
description: Turn Zod schemas into typed database tables. Validate writes, build queries, and connect SQLite, PostgreSQL, MySQL, D1, and more with Zodbase.
---

Zodbase connects Zod schemas to typed tables, queries, and database adaptors. It is under active development and does not yet offer a stable public API.

## Install

```bash
bun add zodbase zod
```

This example uses Bun's built-in SQLite driver. Other databases use their own [adaptors and drivers](/adaptors/).

## Define a table

```ts title="tables.ts"
import { z } from "zod";
import { createTable, metaStore, primaryKey } from "zodbase";

export const Users = createTable({
  id: "users",
  schema: z.object({
    id: z.string().meta(metaStore([primaryKey()])),
    name: z.string(),
    active: z.boolean().default(true),
  }),
});
```

The schema describes the stored values. The table exposes typed field bindings such as `Users.$id` and `Users.$name`.

## Connect and query

```ts title="database.ts"
import { Database as SQLite } from "bun:sqlite";
import { Database } from "zodbase";
import BunSqliteAdaptor from "zodbase/adaptors/bun-sqlite";
import { Users } from "./tables";

const driver = new SQLite(":memory:");
const db = new Database({ adaptor: new BunSqliteAdaptor({ driver }) });

await db.syncTable(Users);
await db.insert(Users, { id: "ada", name: "Ada", active: true });

const { results } = await db.select(Users, ["*"])
  .where(Users.$active.equals(true));

console.log(results);
driver.close();
```

`syncTable` creates a missing table or applies supported schema changes. Read [tables and schemas](/tables-and-schemas/) before using it with existing data.

## Next steps

- Define keys and evolve your [table schema](/tables-and-schemas/).
- Filter and select data with [typed queries](/queries/).
- Choose a [database adaptor](/adaptors/).
- Convert supported [RSQL filters](/rsql/) into conditions.
