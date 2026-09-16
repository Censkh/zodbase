import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ConnectionPool } from "mssql";
import * as z from "zod";
import { backfill, createTable, Database, foreignKey, metaStore, primaryKey, raw } from "../src";
import MssqlAdaptor from "../src/adaptors/mssql";

// CI supplies a disposable SQL Server. Never use an application database:
// every run creates and drops its own randomly named database.
describe.skipIf(!process.env.MSSQL_TEST_URL)("SQL Server integration", () => {
  let admin: ConnectionPool;
  let pool: ConnectionPool;
  let db: Database;
  const name = `zodbase_${crypto.randomUUID().replaceAll("-", "")}`;
  const parent = createTable({
    id: "parent] quoted",
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      value: z.string().nullable(),
      rank: z.number(),
    }),
  });
  const child = createTable({
    id: "child",
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      value: z.string().nullable(),
      active: z.boolean().default(true),
      date: z.date(),
      big: z.bigint(),
      json: z.array(z.string()),
    }),
  });
  const date = new Date("2026-09-16T01:02:03.456Z");
  const values = { active: true, date, big: 9223372036854775807n, json: ["東京", "🦄"] };
  beforeAll(async () => {
    admin = await new ConnectionPool(process.env.MSSQL_TEST_URL!).connect();
    await admin.request().query(`CREATE DATABASE [${name}]`);
    pool = await new ConnectionPool({
      ...ConnectionPool.parseConnectionString(process.env.MSSQL_TEST_URL!),
      database: name,
    }).connect();
    db = new Database({ adaptor: new MssqlAdaptor({ driver: pool }) });
    await db.syncTable(parent);
    await db.syncTable(child);
    await db.insertMany(parent, [
      { id: "a'\\", value: "東京 🦄", rank: 1 },
      { id: "b", value: "second", rank: 2 },
      { id: "null", value: null, rank: 3 },
    ]);
  }, 60_000);
  afterAll(async () => {
    await pool?.close();
    if (admin?.connected) {
      await admin
        .request()
        .query(
          `IF DB_ID('${name}') IS NOT NULL BEGIN ALTER DATABASE [${name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [${name}]; END`,
        );
      await admin.close();
    }
  }, 60_000);
  const source = (id = "a'\\") => db.select(parent, ["value"]).where(parent.$id.equals(id));
  it("creates schemas idempotently and returns Unicode, dates, booleans, bigints and JSON losslessly", async () => {
    await db.syncTable(parent);
    await db.syncTable(child);
    const result = await db.insert(child, { id: "typed", value: source(), ...values }).selectMutated();
    expect(result.first).toEqual({ id: "typed", value: "東京 🦄", ...values });
    expect((await db.select(child, ["*"]).where(child.$id.equals("typed"))).first).toEqual(result.first);
  });
  it("handles ordered pagination and NULL scalar results", async () => {
    const value = db.select(parent, ["value"]).orderBy(parent.$rank, "ASC").offset(1).limit(1);
    expect((await db.insert(child, { id: "offset", value, ...values }).selectMutated()).first?.value).toBe("second");
    for (const id of ["missing", "null"])
      expect((await db.insert(child, { id, value: source(id), ...values }).selectMutated()).first?.value).toBeNull();
    expect((await db.select(parent, ["id"]).offset(1).limit(0)).results).toEqual([]);
  });
  it("rejects multi-row scalar results and atomically rolls back a bulk insert", async () => {
    await expect(
      Promise.resolve(
        db.insertMany(child, [
          { id: "atomic-first", value: source(), ...values },
          { id: "atomic-bad", value: db.select(parent, ["value"]), ...values },
        ]),
      ),
    ).rejects.toThrow();
    expect((await db.select(child, ["id"]).where(child.$id.in(["atomic-first", "atomic-bad"]))).results).toEqual([]);
  });
  it("supports updates, upserts, counts, JSON membership and deletes", async () => {
    await db.insert(child, { id: "crud", value: "before", ...values });
    expect(
      (await db.update(child, { value: "after", active: false }, child.$id.equals("crud")).selectMutated()).first
        ?.active,
    ).toBe(false);
    await db.upsert(child, { id: "crud", value: "upsert", ...values }, child.$id);
    expect((await db.select(child, ["value"]).where(child.$id.equals("crud"))).first?.value).toBe("upsert");
    expect((await db.select(child, ["id"]).where(child.$json.contains("東京"))).results.length).toBeGreaterThan(0);
    expect((await db.count(child)).first?._count).toBeGreaterThan(0);
    await db.delete(child).where(child.$id.equals("crud"));
    expect((await db.select(child, ["id"]).where(child.$id.equals("crud"))).results).toEqual([]);
  });
  it("isolates pooled transactions, sees their own writes, and rolls back", async () => {
    const projection = source("tx-parent");
    await expect(
      db.transaction(async (tx) => {
        await tx.insert(parent, { id: "tx-parent", value: "uncommitted", rank: 4 });
        expect(
          (await tx.insert(child, { id: "tx-child", value: projection, ...values }).selectMutated()).first?.value,
        ).toBe("uncommitted");
        throw new Error("abort transaction");
      }),
    ).rejects.toThrow("abort transaction");
    expect((await projection).results).toEqual([]);
    expect((await db.select(child, ["id"]).where(child.$id.equals("tx-child"))).results).toEqual([]);
  });
  it("enforces unique filtered indexes and foreign key cascades", async () => {
    const relation = createTable({
      id: "relation",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        parentId: z.string().meta(metaStore([foreignKey({ field: parent.$id, onDelete: "cascade" })])),
        label: z.string().nullable(),
      }),
    });
    relation.addIndex("unique_label", [relation.$label], { unique: true, where: relation.$label.notEquals(null) });
    await db.syncTable(relation);
    await db.syncTable(relation);
    await db.insert(parent, { id: "fk-parent", value: "fk", rank: 5 });
    await db.insert(relation, { id: "fk-child", parentId: "fk-parent", label: "unique" });
    await expect(
      Promise.resolve(db.insert(relation, { id: "duplicate", parentId: "fk-parent", label: "unique" })),
    ).rejects.toThrow();
    await db.delete(parent).where(parent.$id.equals("fk-parent"));
    expect((await db.select(relation, ["*"])).results).toEqual([]);
  });
  it("backfills required fields and rolls back an invalid schema migration", async () => {
    const initial = createTable({ id: "migration", schema: z.object({ id: z.string() }) });
    await db.syncTable(initial);
    await db.insert(initial, { id: "existing" });
    const invalid = createTable({ id: "migration", schema: initial.schema.extend({ required: z.string() }) });
    await expect(db.syncTable(invalid)).rejects.toThrow("Backfill value is required");
    const columns = await db.execute(raw("SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(N'migration')"));
    expect(columns.results.map((row: any) => row.name)).toEqual(["id"]);
    const valid = createTable({
      id: "migration",
      schema: initial.schema.extend({ required: z.string().meta(metaStore([backfill({ value: "東京" })])) }),
    });
    await db.syncTable(valid);
    await db.syncTable(valid);
    expect((await db.select(valid, ["*"])).first).toEqual({ id: "existing", required: "東京" });
  });
});
