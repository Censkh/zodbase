import * as z from "zod";
import { backfill, createTable, metaStore, monotonicTimestamp, primaryKey, sql } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

// Inspired by Sequelize's datatype round trips, SQLAlchemy's dialect contracts,
// Drizzle's schema migrations and TypeORM's issue regressions. See COVERAGE.md.
beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

describe.each(TEST_DATABASE_FACTORIES)("edge cases: $name", ({ name: engine, create }) => {
  let context: TestDatabaseContext;
  beforeEach(async () => {
    context = await create();
  });
  afterEach(async () => {
    await context.close();
  });

  const types = [
    {
      name: "numbers",
      schema: z.number(),
      values: [0, -1, Math.PI, 1789021226101, Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER, 1e-100, 1e100],
    },
    { name: "integers", schema: z.int32(), values: [-2147483648, 0, 2147483647] },
    { name: "text", schema: z.string(), values: ["", "réve🐍", "'\\\"%_", "{}", "[]", '{"uploadStatus":"success"}'] },
    { name: "booleans", schema: z.boolean(), values: [true, false] },
    {
      name: "dates",
      schema: z.date(),
      values: [new Date("1970-01-01T00:00:00.000Z"), new Date("2026-09-10T06:20:26.101Z")],
    },
    { name: "bigints", schema: z.bigint(), values: [0n, 9007199254740993n, -9007199254740993n, 9223372036854775807n] },
    {
      name: "objects",
      schema: z.object({ text: z.string(), nested: z.array(z.number().nullable()) }),
      values: [{ text: "'🐍", nested: [1, null, 1789021226101] }],
    },
    { name: "arrays", schema: z.array(z.string()), values: [[], ["'", "{}", "🐍"]] },
    { name: "enums", schema: z.enum(["pending", "success"]), values: ["pending", "success"] },
  ];
  for (const { name, schema, values } of types) {
    it(`round trips ${name} through insert, select and mutation results`, async () => {
      const table = createTable({
        id: "edge_types",
        schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), value: schema.nullable() }),
      });
      await context.db.syncTable(table);
      for (const [index, value] of [...values, null].entries()) {
        const id = String(index);
        expect((await context.db.insert(table, { id, value } as any).selectMutated()).first).toEqual({ id, value });
        expect((await context.db.select(table, ["*"]).where(table.$id.equals(id))).first).toEqual({ id, value });
        expect((await context.db.update(table, { value } as any, table.$id.equals(id)).selectMutated()).first).toEqual({
          id,
          value,
        });
        expect((await context.db.upsert(table, { id, value } as any, table.$id).selectMutated()).first).toEqual({
          id,
          value,
        });
      }
    });
  }

  it("sorts bigints numerically even when the driver needs a text projection", async () => {
    const table = createTable({ id: "edge_bigint_order", schema: z.object({ value: z.bigint() }) });
    await context.db.syncTable(table);
    await context.db.insertMany(
      table,
      [10n, 2n, -10n, -2n, 9007199254740993n].map((value) => ({ value })),
    );
    expect(
      (await context.db.select(table, ["value"]).orderBy(table.$value, "ASC")).results.map((row) => row.value),
    ).toEqual([-10n, -2n, 2n, 10n, 9007199254740993n]);
  });

  it("rejects non-finite numbers and invalid dates without inserting data", async () => {
    const table = createTable({ id: "edge_invalid", schema: z.object({ value: z.number(), date: z.date() }) });
    await context.db.syncTable(table);
    for (const value of [NaN, Infinity, -Infinity]) {
      expect(() => context.db.insert(table, { value, date: new Date() })).toThrow();
    }
    expect(() => context.db.insert(table, { value: 1, date: new Date(NaN) })).toThrow();
    expect((await context.db.select(table, ["*"])).results).toEqual([]);
  });

  it("does not lose revisions under competing writes or duplicate inserts", async () => {
    const table = createTable({
      id: "edge_revisions",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        updatedAt: z.number().meta(metaStore([monotonicTimestamp()])),
        status: z.string(),
      }),
    });
    await context.db.syncTable(table);
    const future = Date.now() + 100_000;
    const inserts = await Promise.allSettled(
      [1, 2].map(() => Promise.resolve(context.db.insert(table, { id: "one", updatedAt: future, status: "pending" }))),
    );
    expect(inserts.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    const revisions = await Promise.all(
      ["uploading", "success", "generating"].map((status) =>
        context.db.transaction(async (db) => {
          await db.update(table, { status }, table.$id.equals("one"));
          return (await db.select(table, ["*"])).first!.updatedAt;
        }),
      ),
    );
    expect(revisions.sort((a, b) => a - b)).toEqual([future + 1, future + 2, future + 3]);
    await expect(
      context.db.transaction(async (db) => {
        await db.update(table, { status: "failed" }, table.$id.equals("one"));
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect((await context.db.select(table, ["*"])).first).toEqual({
      id: "one",
      updatedAt: future + 3,
      status: "generating",
    });
  });

  if (["postgres", "cockroach"].includes(engine)) {
    it("preserves revisions across independent connections and retries serialization conflicts", async () => {
      const peer = await context.connect!();
      try {
        const table = createTable({
          id: "edge_race",
          schema: z.object({
            id: z.string().meta(metaStore([primaryKey()])),
            updatedAt: z.number().meta(metaStore([monotonicTimestamp()])),
          }),
        });
        await context.db.syncTable(table);
        const future = Date.now() + 100_000;
        await context.db.insert(table, { id: "one", updatedAt: future });
        let reads = 0;
        let release!: () => void;
        const bothRead = new Promise<void>((resolve) => {
          release = resolve;
        });
        const revisions = await Promise.all(
          [context.db, peer.db].map((connection) =>
            connection.transaction(async (db) => {
              await db.select(table, ["*"]);
              if (++reads === 2) release();
              await bothRead;
              return (await db.update(table, { updatedAt: 0 }, table.$id.equals("one")).selectMutated()).first
                .updatedAt;
            }),
          ),
        );
        expect(revisions.sort((a, b) => a - b)).toEqual([future + 1, future + 2]);
        expect((await peer.db.select(table, ["*"])).first!.updatedAt).toBe(future + 2);
        if (engine === "cockroach") expect(reads).toBeGreaterThan(2);
      } finally {
        await peer.close();
      }
    }, 30_000);
  }

  if (["mysql", "mariadb"].includes(engine)) {
    it("widens existing datetime columns without losing rows or milliseconds on subsequent writes", async () => {
      await context.db.execute(sql`CREATE TABLE edge_dates (id VARCHAR(255) PRIMARY KEY, value DATETIME NOT NULL)`);
      await context.db.execute(sql`INSERT INTO edge_dates VALUES ('old', '2026-09-10 06:20:26')`);
      const table = createTable({
        id: "edge_dates",
        schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), value: z.date() }),
      });
      await context.db.syncTable(table);
      await context.db.syncTable(table);
      expect((await context.db.select(table, ["*"])).first?.value).toEqual(new Date("2026-09-10T06:20:26Z"));
      const value = new Date("2026-09-10T06:20:26.101Z");
      await context.db.update(table, { value }, table.$id.equals("old"));
      expect((await context.db.select(table, ["*"])).first?.value).toEqual(value);
    });
  }

  it("resumes schema synchronization after an interruption without losing existing data", async () => {
    const original = createTable({
      id: "edge_migration",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), name: z.string() }),
    });
    await context.db.syncTable(original);
    await context.db.insert(original, { id: "one", name: "existing" });
    // The process stopped after adding the nullable column, before its backfill/constraint.
    await context.db.syncTable(
      createTable({ id: "edge_migration", schema: original.schema.extend({ status: z.string().nullable() }) }),
    );
    const migrated = createTable({
      id: "edge_migration",
      schema: original.schema.extend({ status: z.string().meta(metaStore([backfill({ value: "pending" })])) }),
    });
    migrated.addIndex("edge_status", [migrated.$status]);
    await context.db.syncTable(migrated);
    expect((await context.db.select(migrated, ["*"])).first).toEqual({
      id: "one",
      name: "existing",
      status: "pending",
    });
    await context.db.update(migrated, { status: "success" }, migrated.$id.equals("one"));
    await context.db.syncTable(migrated);
    expect((await context.db.select(migrated, ["*"])).first?.status).toBe("success");
  });

  it("pages tied timestamps using an explicit ID tie-breaker despite inserts between pages", async () => {
    const table = createTable({
      id: "edge_pages",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        createdAt: z.number(),
        optional: z.number().nullable(),
      }),
    });
    await context.db.syncTable(table);
    await context.db.insertMany(
      table,
      ["b", "c", "d", "e"].map((id) => ({ id, createdAt: 1789021226101, optional: id === "c" ? null : 1 })),
    );
    const first = await context.db
      .select(table, ["*"])
      .orderBy(table.$createdAt, "ASC")
      .orderBy(table.$id, "ASC")
      .limit(2);
    expect(first.results.map((row) => row.id)).toEqual(["b", "c"]);
    await context.db.insert(table, { id: "a", createdAt: 1789021226101, optional: null });
    const cursor = first.results[1]!;
    const next = await context.db
      .select(table, ["id"])
      .where(
        table.$createdAt
          .greaterThan(cursor.createdAt)
          .or(table.$createdAt.equals(cursor.createdAt).and(table.$id.greaterThan(cursor.id))),
      )
      .orderBy(table.$createdAt, "ASC")
      .orderBy(table.$id, "ASC")
      .limit(2);
    expect(next.results.map((row) => row.id)).toEqual(["d", "e"]);
    expect(
      (await context.db.select(table, ["id"]).where(table.$optional.equals(null)).orderBy(table.$id, "ASC")).results,
    ).toEqual([{ id: "a" }, { id: "c" }]);
    expect((await context.db.select(table, ["id"]).orderBy(table.$id, "ASC").offset(3)).results).toEqual([
      { id: "d" },
      { id: "e" },
    ]);
    const ordered = await context.db.select(table, ["id"]).orderBy(table.$optional, "ASC").orderBy(table.$id, "ASC");
    const nullsLast = engine === "postgres";
    expect(ordered.results.map((row) => row.id)).toEqual(
      nullsLast ? ["b", "d", "e", "a", "c"] : ["a", "c", "b", "d", "e"],
    );
  });
});
