import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as z from "zod";
import { alias, createTable, expression, metaStore, primaryKey, sql, TO_SQL_SYMBOL } from "../src";
import type DatabaseAdaptor from "../src/DatabaseAdaptor";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
} from "./helpers/databaseContract";

const localOnly = process.env.ZODBASE_TEST_LOCAL_ONLY === "1";
if (!localOnly) {
  beforeAll(acquireTestDatabaseContainers, 180_000);
  afterAll(releaseTestDatabaseContainers, 180_000);
}
const factories = TEST_DATABASE_FACTORIES.filter(({ name }) => !localOnly || name === "bun-sqlite");

describe.each(factories)("joins and includes: $name", ({ create }) => {
  it("preserves nullability, decoding, ownership, collection order and parent pagination in one statement", async () => {
    const context = await create();
    const { db } = context;
    const parents = createTable({
      id: "join_parents",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        owner: z.string(),
        region: z.string(),
        label: z.string().nullable(),
      }),
    });
    const children = createTable({
      id: "join_children",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        parentId: z.string(),
        owner: z.string(),
        region: z.string(),
        label: z.string().nullable(),
        position: z.number().int(),
        active: z.boolean(),
        data: z.object({ tags: z.array(z.string()) }),
        exact: z.bigint(),
        at: z.date(),
      }),
    });
    const at = new Date("2026-09-16T12:34:56.000Z");
    try {
      await db.syncTable(parents);
      await db.syncTable(children);
      await db.insertMany(parents, [
        { id: "a", owner: "user", region: "London", label: null },
        { id: "b", owner: "user", region: "Singapore", label: "empty" },
        { id: "c", owner: "other", region: "London", label: "private" },
      ]);
      const data = { tags: ["one", "two"] };
      for (const [id, parentId, owner, position] of [
        ["second", "a", "user", 2],
        ["first", "a", "user", 1],
        ["private", "c", "other", 0],
      ] as const)
        await db.insert(children, {
          id,
          parentId,
          owner,
          region: "London",
          position,
          active: true,
          label: null,
          data,
          exact: 9007199254740993n,
          at,
        });
      const computed = await db
        .select(parents, { id: parents.$id, computed: expression(sql`${"quoted'value"}`, z.string()) })
        .where(parents.$id.equals("a"));
      expect(computed.first).toEqual({ id: "a", computed: "quoted'value" });
      const on = children.$parentId
        .equals(parents.$id)
        .and(children.$owner.equals(parents.$owner))
        .and(children.$region.equals(parents.$region));
      const adaptor = (db as unknown as { options: { adaptor: DatabaseAdaptor } }).options.adaptor;
      const execute = adaptor.execute.bind(adaptor);
      const statements: string[] = [];
      adaptor.execute = (statement) => {
        statements.push(statement[TO_SQL_SYMBOL]());
        return execute(statement);
      };
      const rows = await db
        .select(parents, { parent: parents, child: children })
        .leftJoin(children, on)
        .where(parents.$owner.equals("user"))
        .orderBy(parents.$id, "ASC")
        .orderBy(children.$position, "ASC");
      expect(statements).toHaveLength(1);
      expect(rows.results.map((row) => row.child?.id ?? null)).toEqual(["first", "second", null]);
      expect(rows.results[0]!.child).toMatchObject({ active: true, data, exact: 9007199254740993n, at });
      expect(rows.results[2]!.parent.id).toBe("b");
      const partial = await db
        .select(parents, { parentId: parents.$id, child: { label: children.$label } })
        .leftJoin(children, on)
        .where(parents.$owner.equals("user"))
        .orderBy(parents.$id, "ASC");
      expect(partial.results.map((row) => row.child)).toEqual([{ label: null }, { label: null }, null]);
      expect(
        (await db.select(parents, { id: parents.$id }).innerJoin(children, on).where(parents.$owner.equals("user")))
          .results,
      ).toHaveLength(2);
      expect(
        (await db.select(parents).leftJoin(children, on).where(parents.$owner.equals("user")).count()).first?._count,
      ).toBe(3);
      const childQuery = db.select(children).where(on).orderBy(children.$position, "ASC");
      const query = db
        .select(parents, { id: parents.$id, region: parents.$region })
        .where(parents.$owner.equals("user"))
        .orderBy(parents.$id, "ASC")
        .include({ children: childQuery });
      childQuery.limit(0); // Includes snapshot reusable builders.
      statements.length = 0;
      const included = await query.clone().one();
      expect(statements).toHaveLength(1);
      expect(included.first?.children.map((child) => child.id)).toEqual(["first", "second"]);
      expect(included.first?.children[0]).toMatchObject({ active: true, data, exact: 9007199254740993n, at });
      expect((await query.clone().offset(1).one()).first).toEqual({ id: "b", region: "Singapore", children: [] });
      expect((await query.clone().limit(0)).results).toEqual([]);
      expect((await query.clone().where(parents.$id.equals("missing"))).first).toBeUndefined();
      const pagedChildren = await db
        .select(parents, { id: parents.$id })
        .where(parents.$id.equals("a"))
        .include({
          children: db
            .select(children, { id: children.$id })
            .where(on)
            .orderBy(children.$position, "ASC")
            .offset(1)
            .limit(1),
        });
      expect(pagedChildren.first?.children).toEqual([{ id: "second" }]);
      const other = alias(parents, "other_parent");
      expect(
        (
          await db
            .select(parents, { parent: parents, copy: other })
            .leftJoin(other, other.$id.equals(parents.$id))
            .where(parents.$id.equals("a"))
        ).first?.copy?.id,
      ).toBe("a");
      expect(
        (
          await db
            .select(parents, { id: parents.$id })
            .include({ copies: db.select(other, { id: other.$id }).where(other.$id.equals(parents.$id)) })
            .where(parents.$id.equals("a"))
        ).first?.copies,
      ).toEqual([{ id: "a" }]);
      statements.length = 0;
      const two = await db
        .select(parents, { id: parents.$id })
        .where(parents.$id.equals("a"))
        .include({
          first: db.select(children, { id: children.$id }).where(on),
          second: db.select(children, { id: children.$id }).where(on),
        });
      expect(statements).toHaveLength(1);
      statements.length = 0;
      const grandchild = alias(children, "grandchild");
      const nested = await db
        .select(parents, { id: parents.$id })
        .where(parents.$id.equals("a"))
        .include({
          children: db
            .select(children, { id: children.$id })
            .where(on)
            .orderBy(children.$position, "ASC")
            .include({
              copies: db.select(grandchild, { id: grandchild.$id }).where(grandchild.$id.equals(children.$id)).limit(1),
            }),
        });
      expect(statements).toHaveLength(1);
      expect(nested.first?.children).toEqual([
        { id: "first", copies: [{ id: "first" }] },
        { id: "second", copies: [{ id: "second" }] },
      ]);
      const reservedKeys = await db
        .select(parents, { group: { id: parents.$id, fields: parents.$id, schema: parents.$id } })
        .where(parents.$id.equals("a"));
      expect(reservedKeys.first).toEqual({ group: { id: "a", fields: "a", schema: "a" } });
      const lateJoin = await db
        .select(parents, { p: parents, c: children })
        .leftJoin(children, on)
        .where(parents.$id.equals("b"));
      expect(lateJoin.first?.c).toBeNull();
      const quoted = alias(children, 'child "quoted`alias]');
      expect(
        (
          await db
            .select(parents, { child: quoted })
            .leftJoin(quoted, quoted.$parentId.equals(parents.$id))
            .where(parents.$id.equals("b"))
        ).first?.child,
      ).toBeNull();
      const filteredOn = await db
        .select(parents, { id: parents.$id, child: children })
        .leftJoin(children, on.and(children.$active.equals(false)))
        .where(parents.$owner.equals("user"));
      expect(filteredOn.results).toHaveLength(2);
      expect(filteredOn.results.every((row) => row.child === null)).toBe(true);
      const filteredWhere = await db
        .select(parents, { id: parents.$id })
        .leftJoin(children, on)
        .where(parents.$owner.equals("user").and(children.$active.equals(false)));
      expect(filteredWhere.results).toEqual([]);
      const clonedWithInclude = await query.clone().one();
      expect(clonedWithInclude.first?.children).toHaveLength(2);
      const original = db.select(parents, { child: { label: children.$label } }).leftJoin(children, on);
      const cloned = original.clone().where(parents.$id.equals("b"));
      original.where(parents.$id.equals("a"));
      expect((await cloned).results).toEqual([{ child: null }]);
      expect((await original).results).toEqual([{ child: { label: null } }, { child: { label: null } }]);
      expect(two.first?.first).toHaveLength(2);
      expect(two.first?.second).toHaveLength(2);
      expect(
        (
          await db
            .select(parents, { id: parents.$id })
            .leftJoin(children, on)
            .where(parents.$owner.equals("user' OR 1=1 --"))
        ).results,
      ).toEqual([]);
      await expect(
        Promise.resolve(db.select(parents, { id: parents.$id }).leftJoin(parents, parents.$id.equals("a"))),
      ).rejects.toThrow("alias");
      await expect(Promise.resolve(db.select(parents, { bad: children.$id }))).rejects.toThrow("scope");
      await expect(
        Promise.resolve(
          db.select(parents, { id: parents.$id }).include({ id: db.select(children, { id: children.$id }) }),
        ),
      ).rejects.toThrow("collides");
      await expect(
        Promise.resolve(
          db
            .select(parents, { id: parents.$id })
            .include({ children: db.select(children, { id: children.$id }) })
            .limit(-1),
        ),
      ).rejects.toThrow("limit");
      const copyTable = createTable({
        id: "join_scalar_copy",
        schema: z.object({ id: z.string(), at: z.date(), exact: z.bigint() }),
      });
      await db.syncTable(copyTable);
      await db.insert(copyTable, {
        id: "copy",
        at: db
          .select(children, { at: children.$at })
          .innerJoin(parents, children.$parentId.equals(parents.$id))
          .where(children.$id.equals("first")),
        exact: db
          .select(children, { exact: children.$exact })
          .innerJoin(parents, children.$parentId.equals(parents.$id))
          .where(children.$id.equals("first")),
      });
      expect((await db.select(copyTable)).first).toEqual({ id: "copy", at, exact: 9007199254740993n });
    } finally {
      await context.close();
    }
  }, 60_000);
});
