import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as zod from "zod";
import { add, createTable, type Database, exists, expression, metaStore, primaryKey, sql, withCtes } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const createCounters = () =>
  createTable({
    id: "writes_counter",
    schema: zod.object({
      id: zod.string().meta(metaStore([primaryKey()])),
      name: zod.string(),
      count: zod.number(),
    }),
  });

const createCopies = () =>
  createTable({
    id: "writes_copy",
    schema: zod.object({
      id: zod.string().meta(metaStore([primaryKey()])),
      label: zod.string(),
    }),
  });

const counters = [
  { id: "a", name: "Apples", count: 1 },
  { id: "b", name: "Bananas", count: 2 },
  { id: "c", name: "Cherries", count: 3 },
];

describe.each(TEST_DATABASE_FACTORIES)("writes: $name", ({ name, create }) => {
  let context: TestDatabaseContext;
  let db: Database;
  let Counters: ReturnType<typeof createCounters>;
  let Copies: ReturnType<typeof createCopies>;
  const mysql = name === "mysql" || name === "mariadb";
  const postgres = name === "postgres" || name === "cockroach";

  const stored = async () => (await db.select(Counters).orderBy(Counters.$id, "ASC")).results;

  beforeEach(async () => {
    context = await create();
    db = context.db as Database;
    Counters = createCounters();
    Copies = createCopies();
    await db.syncTable(Counters);
    await db.syncTable(Copies);
  });

  afterEach(async () => {
    await context.close();
  });

  it("inserts one row or several, returning the rows written or just the keys asked for", async () => {
    const one = await db.insert(Counters, counters[0]!).returning();
    expect(one.first).toEqual(counters[0]!);
    const many = await db.insert(Counters, counters.slice(1)).returning("id");
    expect(many.results).toEqual([{ id: "b" }, { id: "c" }]);
    expect(await stored()).toEqual(counters);
  });

  it("skips rows that conflict on the key, keeping the existing ones", async () => {
    await db.insert(Counters, counters[0]!);
    await db
      .insert(Counters, [{ ...counters[0]!, name: "Changed" }, counters[1]!])
      .onConflict(Counters.$id)
      .doNothing();
    expect(await stored()).toEqual(counters.slice(0, 2));
  });

  it.skipIf(mysql)("returns only the rows an insert that skips conflicts wrote", async () => {
    await db.insert(Counters, counters[0]!);
    const inserted = await db
      .insert(Counters, [{ ...counters[0]!, name: "Changed" }, counters[1]!])
      .onConflict(Counters.$id)
      .doNothing()
      .returning("id");
    expect(inserted.results).toEqual([{ id: "b" }]);
  });

  it("updates the rows that conflict on the key to the inserted values", async () => {
    await db.insert(Counters, counters.slice(0, 2));
    const upserted = await db
      .insert(Counters, { id: "a", name: "Avocados", count: 9 })
      .onConflict(Counters.$id)
      .doUpdate()
      .returning();
    expect(upserted.first).toEqual({ id: "a", name: "Avocados", count: 9 });
    await db
      .insert(Counters, [
        { id: "b", name: "Blueberries", count: 8 },
        { id: "c", name: "Cherries", count: 3 },
      ])
      .onConflict(Counters.$id)
      .doUpdate();
    expect(await stored()).toEqual([
      { id: "a", name: "Avocados", count: 9 },
      { id: "b", name: "Blueberries", count: 8 },
      { id: "c", name: "Cherries", count: 3 },
    ]);
  });

  it("upserts rows that hold only their key, returning the existing row", async () => {
    const Keys = createTable({
      id: "writes_key",
      schema: zod.object({ id: zod.string().meta(metaStore([primaryKey()])) }),
    });
    await db.syncTable(Keys);
    await db.insert(Keys, { id: "a" });
    const upserted = await db.insert(Keys, { id: "a" }).onConflict(Keys.$id).doUpdate().returning();
    expect(upserted.results).toEqual([{ id: "a" }]);
    await db
      .insert(Keys, [{ id: "a" }, { id: "b" }])
      .onConflict(Keys.$id)
      .doUpdate();
    expect((await db.select(Keys).orderBy(Keys.$id, "ASC")).results).toEqual([{ id: "a" }, { id: "b" }]);
  });

  it("updates only the rows every where condition matches, returning them", async () => {
    await db.insert(Counters, counters);
    const updated = await db
      .update(Counters, { name: "Big" })
      .where(Counters.$count.greaterThan(1))
      .where(Counters.$id.notEquals("c"))
      .returning("id", "name");
    expect(updated.results).toEqual([{ id: "b", name: "Big" }]);
    expect((await stored()).map((row) => row.name)).toEqual(["Apples", "Big", "Cherries"]);
  });

  it("refuses to update or delete without a where condition", async () => {
    await db.insert(Counters, counters);
    await expect(Promise.resolve(db.update(Counters, { name: "All" }))).rejects.toThrow("without where condition");
    await expect(Promise.resolve(db.delete(Counters))).rejects.toThrow("without where condition");
    expect(await stored()).toEqual(counters);
  });

  it("increments a counter in place with add", async () => {
    await db.insert(Counters, counters);
    await db.update(Counters, { count: add(Counters.$count, 10) }).where(Counters.$id.equals("b"));
    expect((await stored()).map((row) => row.count)).toEqual([1, 12, 3]);
  });

  it("updates each row matching its key to its own values", async () => {
    await db.insert(Counters, counters);
    await db
      .update(Counters, [
        { id: "a", count: 5 },
        { id: "c", count: 7 },
      ])
      .by(Counters.$id);
    expect((await stored()).map((row) => row.count)).toEqual([5, 2, 7]);
  });

  it("deletes the rows its where conditions match", async () => {
    await db.insert(Counters, counters);
    await db.delete(Counters).where(Counters.$count.greaterThan(1)).where(Counters.$id.notEquals("b"));
    expect((await stored()).map((row) => row.id)).toEqual(["a", "b"]);
  });

  it("counts matching rows, reading no more than upTo", async () => {
    await db.insert(Counters, counters);
    expect(await db.select(Counters).count()).toBe(3);
    expect(await db.select(Counters).where(Counters.$count.greaterThan(1)).count()).toBe(2);
    expect(await db.select(Counters).count({ upTo: 2 })).toBe(2);
  });

  it("inserts what a select gives, its projection naming the columns", async () => {
    await db.insert(Counters, counters);
    await db
      .insert(Copies)
      .fromSelect(db.select(Counters, { id: Counters.$id, label: Counters.$name }).where(Counters.$count.lessThan(3)));
    expect((await db.select(Copies).orderBy(Copies.$id, "ASC")).results).toEqual([
      { id: "a", label: "Apples" },
      { id: "b", label: "Bananas" },
    ]);
  });

  it("gives each write in a batch the result awaiting it alone would", async () => {
    await db.insert(Counters, counters[0]!);
    const [inserted, updated, deleted] = await db.batch([
      db.insert(Counters, counters.slice(1)).returning("id"),
      db.update(Counters, { count: 4 }).where(Counters.$id.equals("a")),
      db.delete(Counters).where(Counters.$id.equals("c")),
    ]);
    expect(inserted.results).toEqual([{ id: "b" }, { id: "c" }]);
    expect(updated).toEqual({ results: [], first: undefined });
    expect(deleted).toEqual({ results: [], first: undefined });
    expect(await stored()).toEqual([
      { id: "a", name: "Apples", count: 4 },
      { id: "b", name: "Bananas", count: 2 },
    ]);
  });

  it("writes a batch atomically: when one write fails, none is stored", async () => {
    await db.insert(Counters, counters[0]!);
    await expect(
      db.batch([
        db.insert(Counters, counters[1]!),
        db.update(Counters, { count: 50 }).where(Counters.$id.equals("a")),
        // The key is taken: this insert fails, and the batch with it.
        db.insert(Counters, counters[0]!),
      ]),
    ).rejects.toThrow();
    expect(await stored()).toEqual([counters[0]!]);
  });

  it.skipIf(!postgres)("sends a batch in one round trip", async () => {
    const adaptor = (db as unknown as { options: { adaptor: { driver: { query(...args: unknown[]): unknown } } } })
      .options.adaptor;
    const query = spyOn(adaptor.driver, "query");
    await db.batch([
      db.insert(Counters, counters[0]!),
      db.insert(Counters, counters.slice(1)),
      db.update(Counters, { count: 6 }).where(Counters.$id.equals("b")),
    ]);
    expect(query).toHaveBeenCalledTimes(1);
    query.mockRestore();
    expect((await stored()).map((row) => row.count)).toEqual([1, 6, 3]);
  });

  it.skipIf(!postgres)("composes writes into writable CTEs", async () => {
    await db.insert(Counters, counters);
    const Updated = createTable({ id: "updated_counter", schema: Counters.schema });
    const update = db
      .update(Counters, { count: 0 })
      .where(Counters.$id.in(["a", "b"]))
      .returning();
    const copy = db
      .insert(Copies)
      .fromSelect(db.select(Updated, { id: Updated.$id, label: Updated.$name }))
      .returning("id");
    const Copied = createTable({ id: "copied", schema: zod.object({ id: zod.string() }) });
    const { results } = await db.execute(
      withCtes(
        [
          { table: Updated, query: update },
          { table: Copied, query: copy },
        ],
        db
          .select(Updated, { id: Updated.$id, copied: expression(sql`true`, zod.boolean()) })
          .where(exists(db.select(Copied, { id: Copied.$id })))
          .orderBy(Updated.$id, "ASC"),
      ),
    );
    expect(results.map((row) => row.id)).toEqual(["a", "b"]);
    expect((await db.select(Copies).orderBy(Copies.$id, "ASC")).results).toEqual([
      { id: "a", label: "Apples" },
      { id: "b", label: "Bananas" },
    ]);
    expect((await stored()).map((row) => row.count)).toEqual([0, 0, 3]);
  });
});
