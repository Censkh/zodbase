import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as z from "zod";
import { createTable, metaStore, primaryKey } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

describe.each(TEST_DATABASE_FACTORIES)("scalar edge contracts: $name", ({ name, create }) => {
  it("preserves SQL NULL, pagination, column alignment and transaction rollback", async () => {
    const { db, close } = await create();
    const parent = createTable({
      id: "edge_parent",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        value: z.string().nullable(),
        owner: z.string(),
        rank: z.number(),
      }),
    });
    const child = createTable({
      id: "edge_child",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        value: z.string().nullable().default("fallback"),
        label: z.string().default("default"),
        extra: z.string().nullish(),
      }),
    });
    try {
      await db.syncTable(parent);
      await db.syncTable(child);
      await db.insertMany(parent, [
        { id: "a", value: "東京 🦄", owner: "alice", rank: 1 },
        { id: "b", value: null, owner: "bob", rank: 2 },
      ]);
      const source = () => db.select(parent, ["value"]).where(parent.$id.equals("a"));
      await db.insertMany(child, [
        { value: source(), id: "aligned-first" },
        { extra: "present", label: "custom", id: "aligned-second", value: "literal" },
        { id: "missing", value: db.select(parent, ["value"]).where(parent.$id.equals("absent")) },
        { id: "wrong-owner", value: source().where(parent.$owner.equals("bob")) },
        { id: "sql-null", value: db.select(parent, ["value"]).where(parent.$id.equals("b")) },
        { id: "offset", value: db.select(parent, ["value"]).orderBy(parent.$rank, "DESC").offset(1).limit(1) },
        { id: "zero", value: source().limit(0) },
      ]);
      const rows = Object.fromEntries((await db.select(child, ["*"])).results.map((row) => [row.id, row]));
      expect(rows["aligned-first"]).toEqual({ id: "aligned-first", value: "東京 🦄", label: "default", extra: null });
      expect(rows["aligned-second"]).toEqual({
        id: "aligned-second",
        value: "literal",
        label: "custom",
        extra: "present",
      });
      for (const id of ["missing", "wrong-owner", "sql-null", "zero"]) expect(rows[id]?.value).toBeNull();
      expect(rows.offset?.value).toBe("東京 🦄");
      if (!["bun-sqlite", "turso-local"].includes(name)) {
        await expect(
          Promise.resolve(
            db.insertMany(child, [
              { id: "atomic-good", value: source() },
              { id: "atomic-bad", value: db.select(parent, ["value"]) },
            ]),
          ),
        ).rejects.toThrow();
        expect((await db.select(child, ["id"]).where(child.$id.in(["atomic-good", "atomic-bad"]))).results).toEqual([]);
      }
      const uncommitted = db.select(parent, ["value"]).where(parent.$id.equals("tx"));
      await expect(
        db.transaction(async (tx) => {
          await tx.insert(parent, { id: "tx", value: "uncommitted", owner: "alice", rank: 3 });
          await tx.insert(child, { id: "tx-child", value: uncommitted });
          expect((await tx.select(child, ["value"]).where(child.$id.equals("tx-child"))).first?.value).toBe(
            "uncommitted",
          );
          throw new Error("rollback edge test");
        }),
      ).rejects.toThrow("rollback edge test");
      expect((await db.select(child, ["id"]).where(child.$id.equals("tx-child"))).results).toEqual([]);
    } finally {
      await close();
    }
  });
});
