---
title: Joins and includes
description: Join typed tables and load ordered related collections in one SQL statement.
---

## Join related rows

Use field references in `equals()` to compare columns. Join conditions are explicit; foreign-key metadata does not load relations automatically.

```ts
const { results } = await db
  .select(Users, { user: Users, post: Posts })
  .leftJoin(Posts, Posts.$authorId.equals(Users.$id))
  .where(Users.$id.equals(userId))
  .orderBy(Posts.$createdAt, "DESC");
```

`leftJoin()` keeps users without posts; their `post` is `null`. `innerJoin()` returns only matching rows. A one-to-many join produces one result per matching child, repeating the parent. `.limit()`, `.offset()`, and `.count()` apply to these joined rows, not distinct parents.

Pass a projection object to `select()` for named, nested projections. Tables select all their columns; field bindings select individual values:

```ts
db.select(Users, { userId: Users.$id, post: { title: Posts.$title } });
```

An absent left-joined post is `null`, distinguishable from a present post whose title is null. Nested selections from a single left-joined table are nullable in TypeScript. Mixed-table objects preserve the nullability of their individual fields. JSON, booleans, dates and big integers use the adaptor's normal decoding.

Keep ownership and locality constraints explicit, particularly when IDs alone are insufficient:

```ts
.leftJoin(Assets, Assets.$layerId.equals(Layers.$id)
  .and(Assets.$userId.equals(Layers.$userId))
  .and(Assets.$homeRegion.equals(Layers.$homeRegion)))
.where(Layers.$userId.equals(userId))
```

Put child restrictions in the join's `on` condition when unmatched parents must remain visible. Putting them in `where()` can remove those parents.

## Include a collection without duplicating parents

```ts
const { results } = await db
  .select(Users, { id: Users.$id, name: Users.$name })
  .orderBy(Users.$id, "ASC")
  .limit(20)
  .include({
    posts: db.select(Posts, { id: Posts.$id, title: Posts.$title })
      .where(Posts.$authorId.equals(Users.$id))
      .orderBy(Posts.$createdAt, "DESC"),
  });
```

Each user has a `posts` array, empty when there are no matches. Includes compile to correlated SQL collections in **one statement**, not one query per parent. Parent pagination remains parent pagination. Limits and offsets on the child query apply per parent. Include keys must not collide with selected fields. Builders are snapshotted when included; clone the outer builder before reusing it with different pagination.

Adaptors generate database-specific JSON collection SQL and decode it back to typed objects. PostgreSQL/CockroachDB use JSON aggregation; SQLite/D1 use JSON group aggregation; MySQL/MariaDB use their JSON aggregation syntax; SQL Server uses `FOR JSON PATH`. This avoids application round trips, but still requires suitable indexes on child lookup and ordering columns.

## Self-joins and aliases

```ts
import { alias } from "zodbase";

const Managers = alias(Users, "manager");
const query = db.select(Users, { employee: Users, manager: Managers })
  .leftJoin(Managers, Users.$managerId.equals(Managers.$id));
```

Use distinct aliases when the same table appears more than once, including correlated child queries. Referenced fields must belong to the current query or an enclosing include's scope.

## SQL expressions in a projection

```ts
import { expression, sql } from "zodbase";
import { z } from "zod";

const result = await db.select(Users, {
    user: Users,
    gatewayRegion: expression(sql`gateway_region()`, z.string()),
  });
```

`expression(statement, schema)` adds trusted SQL to the selected fields and uses the schema for driver-value decoding and TypeScript inference. It does not translate database functions or validate that your schema matches their result. This example is CockroachDB-specific; do not execute it on SQLite/D1. SQL template values use Zodbase's normal escaping; never build raw expressions from untrusted strings.
