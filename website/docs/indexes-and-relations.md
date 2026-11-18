---
title: Indexes and foreign keys
description: Add compound and partial indexes, declare foreign keys, and configure delete actions in Zodbase.
---

## Add an index

Attach indexes to the table before synchronization:

```ts
Users.addIndex("users_name_idx", [Users.$name]);
await db.syncTable(Users);
```

Names should be stable across deployments. A compound index takes multiple field bindings in the desired order.

## Unique and partial indexes

```ts
Users.addIndex("users_active_name_unique", [Users.$name], {
  unique: true,
  where: Users.$active.equals(true),
});
```

This asks for uniqueness among active users. Partial indexes depend on the adaptor and database; MySQL does not support a `WHERE` predicate on an index. Existing duplicate data must be resolved before adding a unique index. Review and apply index changes through `syncTable`.

Synchronizing creates an index the database lacks, and rebuilds one whose definition changed under the same name: its columns, their order, its uniqueness or its condition. Rebuilding drops the index and creates it again, so its table goes without it in between. Zodbase records each index's definition where the database keeps it: SQLite keeps an index's statement, and PostgreSQL and CockroachDB get it as the index's comment. An index made before Zodbase recorded its definition is compared by its columns, uniqueness and whether it has a condition. On CockroachDB, rebuilding a unique index also drops a foreign key that relies on it, which synchronizing the referencing table adds again. An index the table no longer declares stays: dropping one is a migration's.

## Declare a foreign key

```ts
import { z } from "zod";
import { createTable, foreignKey, metaStore, primaryKey } from "zodbase";

const Posts = createTable({
  id: "posts",
  schema: z.object({
    id: z.string().meta(metaStore([primaryKey()])),
    authorId: z.string().meta(metaStore([
      foreignKey({ field: Users.$id, onDelete: "cascade" }),
    ])),
    title: z.string(),
  }),
});

await db.syncTable(Users);
await db.syncTable(Posts);
```

Synchronize the referenced table first. Foreign-key metadata declares a database constraint; it does not automatically load related rows. Use explicit [joins and includes](/joins) with field-to-field conditions.

## Choose a delete action

| Action | Intent |
| --- | --- |
| `no action` | Default behavior; the database checks the constraint. |
| `restrict` | Reject a referenced-row deletion that would break the relation. |
| `cascade` | Delete dependent rows with the referenced row. |
| `set null` | Clear the reference; the child field must permit null. |
| `set default` | Use the column default where supported by the database. |

Actual support and timing of constraint checks depend on the database. Zodbase's schema contracts cover adding, changing, and removing foreign keys, including SQLite table rebuilds. Review existing rows before introducing a new constraint.
