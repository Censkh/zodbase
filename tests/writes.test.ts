import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as zod from "zod";
import {
  add,
  createTable,
  type Database,
  exists,
  expression,
  incoming,
  metaStore,
  notExists,
  primaryKey,
  sql,
  sum,
  union,
  unionAll,
  values,
} from "../src";
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

  it("updates and deletes the rows a condition holding a select matches", async () => {
    await db.insert(Counters, counters);
    await db.insert(Copies, [{ id: "a", label: "Apples" }]);
    const copied = db.select(Copies, { id: Copies.$id });
    await db.update(Counters, { count: 0 }).where(Counters.$id.in(copied));
    await db.delete(Counters).where(Counters.$id.notIn(copied).and(Counters.$count.greaterThan(2)));
    expect(await stored()).toEqual([
      { id: "a", name: "Apples", count: 0 },
      { id: "b", name: "Bananas", count: 2 },
    ]);
  });

  it("groups rows, summing each group", async () => {
    await db.insert(Counters, [...counters, { id: "d", name: "Apples", count: 10 }]);
    const { results } = await db
      .select(Counters, { name: Counters.$name, total: sum(Counters.$count) })
      .groupBy(Counters.$name)
      .orderBy(Counters.$name, "ASC");
    expect(results).toEqual([
      { name: "Apples", total: 11 },
      { name: "Bananas", total: 2 },
      { name: "Cherries", total: 3 },
    ]);
  });

  it("reads literal rows as a table, pairing them with a query's rows by a cross join", async () => {
    await db.insert(Counters, counters);
    const Labels = createTable({
      id: "writes_labels",
      schema: zod.object({ suffix: zod.string(), position: zod.number().int() }),
    });
    const labels = values(Labels, [
      { suffix: "one", position: 1 },
      { suffix: "two", position: 2 },
    ]);
    expect((await db.select(labels).orderBy(labels.$position, "ASC")).results).toEqual([
      { suffix: "one", position: 1 },
      { suffix: "two", position: 2 },
    ]);
    await db
      .insert(Copies)
      .select(
        db
          .select(Counters, { id: labels.$suffix, label: Counters.$name })
          .crossJoin(labels)
          .where(Counters.$id.equals("a")),
      );
    expect((await db.select(Copies).orderBy(Copies.$id, "ASC")).results).toEqual([
      { id: "one", label: "Apples" },
      { id: "two", label: "Apples" },
    ]);
    expect((await db.select(values(Labels, []))).results).toEqual([]);
  });

  it("matches rows where a correlated query gives no row", async () => {
    await db.insert(Counters, counters);
    await db.insert(Copies, { id: "b", label: "Bananas" });
    const { results } = await db
      .select(Counters, { id: Counters.$id })
      .where(notExists(db.select(Copies, { id: Copies.$id }).where(Copies.$id.equals(Counters.$id))))
      .orderBy(Counters.$id, "ASC");
    expect(results).toEqual([{ id: "a" }, { id: "c" }]);
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
      .select(db.select(Counters, { id: Counters.$id, label: Counters.$name }).where(Counters.$count.lessThan(3)));
    expect((await db.select(Copies).orderBy(Copies.$id, "ASC")).results).toEqual([
      { id: "a", label: "Apples" },
      { id: "b", label: "Bananas" },
    ]);
  });

  it("skips or updates the selected rows that conflict with existing ones", async () => {
    await db.insert(Copies, { id: "a", label: "Kept" });
    const select = db.select(Counters, { id: Counters.$id, label: Counters.$name });
    await db.insert(Counters, counters);
    await db
      .insert(Copies)
      .select(select.clone().where(Counters.$id.in(["a", "b"])))
      .onConflict(Copies.$id)
      .doNothing();
    expect((await db.select(Copies).orderBy(Copies.$id, "ASC")).results).toEqual([
      { id: "a", label: "Kept" },
      { id: "b", label: "Bananas" },
    ]);
    await db.insert(Copies).select(select.clone()).onConflict(Copies.$id).doUpdate();
    expect((await db.select(Copies).orderBy(Copies.$id, "ASC")).results).toEqual([
      { id: "a", label: "Apples" },
      { id: "b", label: "Bananas" },
      { id: "c", label: "Cherries" },
    ]);
  });

  it("updates a conflicting row to values combining it with the incoming row", async () => {
    await db.insert(Counters, counters);
    // The existing count plus the inserted one; the name stays.
    const increment = { count: add(Counters.$count, incoming(Counters.$count)) };
    await db.insert(Counters, { id: "a", name: "Ignored", count: 10 }).onConflict(Counters.$id).doUpdate(increment);
    await db
      .insert(Counters, [
        { id: "b", name: "Ignored", count: 20 },
        { id: "d", name: "Dates", count: 4 },
      ])
      .onConflict(Counters.$id)
      .doUpdate(increment);
    expect(await stored()).toEqual([
      { id: "a", name: "Apples", count: 11 },
      { id: "b", name: "Bananas", count: 22 },
      { id: "c", name: "Cherries", count: 3 },
      { id: "d", name: "Dates", count: 4 },
    ]);
  });

  it.skipIf(mysql)("names a partial unique index as the conflict target", async () => {
    const Labels = createTable({
      id: "writes_label",
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        label: zod.string(),
        current: zod.boolean(),
      }),
    });
    // One current row per label; past ones may share it.
    Labels.addIndex("writes_label_current", [Labels.$label], { unique: true, where: Labels.$current.equals(true) });
    await db.syncTable(Labels);
    await db.insert(Labels, [
      { id: "old", label: "x", current: false },
      { id: "now", label: "x", current: true },
    ]);
    await db
      .insert(Labels, { id: "new", label: "x", current: true })
      .onConflict(Labels.$label, { where: Labels.$current.equals(true) })
      .doUpdate({ current: false });
    await db
      .insert(Labels, { id: "other", label: "y", current: true })
      .onConflict(Labels.$label, {
        where: Labels.$current.equals(true),
      })
      .doNothing();
    expect((await db.select(Labels).orderBy(Labels.$id, "ASC")).results).toEqual([
      { id: "now", label: "x", current: false },
      { id: "old", label: "x", current: false },
      { id: "other", label: "y", current: true },
    ]);
  });

  it("reads a named query's rows through its table, typed, and counts them", async () => {
    await db.insert(Counters, counters);
    const Small = createTable({ id: "small_counter", schema: Counters.schema });
    const scoped = db.with(Small, db.select(Counters).where(Counters.$count.lessThan(3)));
    expect((await scoped.select(Small).orderBy(Small.$id, "ASC")).results).toEqual(counters.slice(0, 2));
    expect(await scoped.select(Small).count()).toBe(2);
    expect(await scoped.select(Small).count({ upTo: 1 })).toBe(1);
    // A select of the scope used as a subquery takes its WITH along.
    expect(
      (
        await db.select(Counters, { id: Counters.$id }).where(Counters.$id.in(scoped.select(Small, { id: Small.$id })))
      ).results
        .map((row) => row.id)
        .toSorted(),
    ).toEqual(["a", "b"]);
  });

  it("keeps the duplicates of a union all, which a union removes", async () => {
    await db.insert(Counters, counters);
    const Names = createTable({ id: "writes_names", schema: zod.object({ name: zod.string() }) });
    const apples = db.select(Counters, { name: Counters.$name }).where(Counters.$id.equals("a"));
    const all = await db.with(Names, unionAll(apples, apples.clone())).select(Names);
    const distinct = await db.with(Names, union(apples, apples.clone())).select(Names);
    expect(all.results).toEqual([{ name: "Apples" }, { name: "Apples" }]);
    expect(distinct.results).toEqual([{ name: "Apples" }]);
  });

  it.skipIf(postgres)("refuses a write after a WITH clause where the database can't build one", async () => {
    await db.insert(Counters, counters);
    const Small = createTable({ id: "small_counter", schema: Counters.schema });
    const scoped = db.with(Small, db.select(Counters).where(Counters.$count.lessThan(3)));
    await expect(
      Promise.resolve(scoped.delete(Counters).where(Counters.$id.in(scoped.select(Small, { id: Small.$id })))),
    ).rejects.toThrow("after a WITH clause");
    expect(await stored()).toEqual(counters);
  });

  it("inserts rows only where a condition holds", async () => {
    await db.insert(Counters, counters[0]!);
    const apples = exists(db.select(Counters, { id: Counters.$id }).where(Counters.$id.equals("a")));
    const zucchini = exists(db.select(Counters, { id: Counters.$id }).where(Counters.$id.equals("z")));
    await db
      .insert(Copies, [
        { id: "x", label: "First" },
        { id: "y", label: "Second" },
      ])
      .where(apples);
    await db.insert(Copies, { id: "z", label: "Never" }).where(zucchini);
    // A conflicting row is skipped as an insert of values skips it.
    await db
      .insert(Copies, [
        { id: "x", label: "Again" },
        { id: "w", label: "New" },
      ])
      .where(apples)
      .onConflict(Copies.$id)
      .doNothing();
    expect((await db.select(Copies).orderBy(Copies.$id, "ASC")).results).toEqual([
      { id: "w", label: "New" },
      { id: "x", label: "First" },
      { id: "y", label: "Second" },
    ]);
  });

  it.skipIf(mysql)("returns the rows a conditional insert wrote, and none where it held for none", async () => {
    await db.insert(Counters, counters[0]!);
    const held = exists(db.select(Counters, { id: Counters.$id }).where(Counters.$id.equals("a")));
    const missing = exists(db.select(Counters, { id: Counters.$id }).where(Counters.$id.equals("z")));
    expect((await db.insert(Copies, { id: "x", label: "Kept" }).where(held).returning("id")).results).toEqual([
      { id: "x" },
    ]);
    expect((await db.insert(Copies, { id: "y", label: "Lost" }).where(missing).returning("id")).results).toEqual([]);
  });

  it("upserts a row whose values include a subquery", async () => {
    await db.insert(Counters, counters);
    const name = (id: string) => db.select(Counters, { label: Counters.$name }).where(Counters.$id.equals(id));
    await db
      .insert(Copies, { id: "c", label: name("a") })
      .onConflict(Copies.$id)
      .doUpdate();
    await db
      .insert(Copies, { id: "c", label: name("b") })
      .onConflict(Copies.$id)
      .doUpdate();
    expect((await db.select(Copies)).results).toEqual([{ id: "c", label: "Bananas" }]);
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
      .select(db.select(Updated, { id: Updated.$id, label: Updated.$name }))
      .returning("id");
    const Copied = createTable({ id: "copied", schema: zod.object({ id: zod.string() }) });
    const { results } = await db
      .with(Updated, update)
      .with(Copied, copy)
      .select(Updated, { id: Updated.$id, copied: expression(sql`true`, zod.boolean()) })
      .where(exists(db.select(Copied, { id: Copied.$id })))
      .orderBy(Updated.$id, "ASC");
    expect(results).toEqual([
      { id: "a", copied: true },
      { id: "b", copied: true },
    ]);
    expect((await db.select(Copies).orderBy(Copies.$id, "ASC")).results).toEqual([
      { id: "a", label: "Apples" },
      { id: "b", label: "Bananas" },
    ]);
    expect((await stored()).map((row) => row.count)).toEqual([0, 0, 3]);
  });
});
