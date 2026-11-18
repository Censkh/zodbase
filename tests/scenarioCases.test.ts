import * as z from "zod";
import { add, createTable, divide, incoming, metaStore, notExists, primaryKey, scalar, sum, union } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

// Scenarios from Kysely's integration tests and SQLAlchemy's dialect compliance suite, both written
// to run unchanged on every database they support.
beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const createItems = () =>
  createTable({
    id: "scenario_item",
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      group: z.string(),
      count: z.int(),
      parts: z.int(),
      data: z.object({ text: z.string(), list: z.array(z.string().nullable()) }).nullable(),
    }),
  });

describe.each(TEST_DATABASE_FACTORIES)("scenarios: $name ($connection)", ({ name: engine, create }) => {
  let context: TestDatabaseContext;
  let Items: ReturnType<typeof createItems>;
  beforeEach(async () => {
    context = await create();
    Items = createItems();
    await context.db.syncTable(Items);
  });
  afterEach(async () => {
    await context.close();
  });
  const item = (id: string, values: Partial<z.input<typeof Items.schema>> = {}) => ({
    id,
    group: "g",
    count: 0,
    parts: 1,
    data: null,
    ...values,
  });
  const mysql = engine === "mysql" || engine === "mariadb";
  const ids = async () =>
    (await context.db.select(Items, { id: Items.$id }).orderBy(Items.$id, "ASC")).results.map((row) => row.id);

  // Kysely, transaction.test.ts
  it("rolls back a transaction whose callback caught a failed statement and went on", async () => {
    await context.db.insert(Items, item("a"));
    let caught: unknown;
    const attempt = context.db.transaction(async (tx) => {
      await tx.insert(Items, item("b"));
      try {
        await tx.insert(Items, item("a"));
      } catch (error) {
        caught = error;
      }
      await tx.insert(Items, item("c"));
    });
    // It rejects with the statement's own error, though PostgreSQL then refuses the next statement.
    const rejected = await attempt.then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(caught).toBeInstanceOf(Error);
    expect(rejected).toBe(caught);
    expect(await ids()).toEqual(["a"]);
  });

  it("refuses statements on a transaction after it has ended", async () => {
    let kept: TestDatabaseContext["db"] | undefined;
    await context.db.transaction(async (tx) => {
      kept = tx;
    });
    await expect((async () => await kept!.select(Items))()).rejects.toThrow("transaction has ended");
    await expect((async () => await kept!.insert(Items, item("late")))()).rejects.toThrow("transaction has ended");
    expect(await ids()).toEqual([]);
  });

  // SQLAlchemy, test_cte.py and test_update_delete.py. MySQL and MariaDB refuse a write whose
  // condition reads the table it writes ("can't specify target table").
  it.skipIf(mysql)("updates and deletes rows by a condition that reads their own table", async () => {
    await context.db.insert(Items, [item("a", { count: 1 }), item("b", { count: 2 }), item("c", { count: 3 })]);
    const total = scalar(context.db.select(Items, { total: sum(Items.$count) }), z.number());
    await context.db.update(Items, { parts: 9 }).where(Items.$count.greaterThan(divide(total, 3)));
    expect(
      (await context.db.select(Items, { id: Items.$id }).where(Items.$parts.equals(9))).results.map((row) => row.id),
    ).toEqual(["c"]);
    const largest = context.db.select(Items, { id: Items.$id }).orderBy(Items.$count, "DESC").limit(1);
    await context.db.delete(Items).where(Items.$id.in(largest));
    expect(await ids()).toEqual(["a", "b"]);
  });

  it("returns a row an update leaves as it was, and nothing from writes that match no row", async () => {
    await context.db.insert(Items, item("a", { count: 5 }));
    const same = await context.db.update(Items, { count: 5 }).where(Items.$id.equals("a")).returning("id", "count");
    expect(same.results).toEqual([{ id: "a", count: 5 }]);
    const none = Items.$id.equals("missing");
    expect((await context.db.update(Items, { count: 1 }).where(none).returning("id")).results).toEqual([]);
    expect(await context.db.select(Items).where(none).count()).toBe(0);
  });

  it.skipIf(mysql)("returns the rows a delete removed, and none from one that matches no row", async () => {
    await context.db.insert(Items, [item("a"), item("b")]);
    expect((await context.db.delete(Items).where(Items.$id.equals("missing")).returning("id")).results).toEqual([]);
    expect((await context.db.delete(Items).where(Items.$id.equals("a")).returning("id")).results).toEqual([
      { id: "a" },
    ]);
    expect(await ids()).toEqual(["b"]);
  });

  it("matches no row with NOT IN over a subquery that gives a null, as SQL does", async () => {
    const Others = createTable({
      id: "scenario_other",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), group: z.string().nullable() }),
    });
    await context.db.syncTable(Others);
    await context.db.insert(Items, [item("a", { group: "x" }), item("b", { group: "y" })]);
    await context.db.insert(Others, [
      { id: "1", group: "x" },
      { id: "2", group: null },
    ]);
    const groups = context.db.select(Others, { group: Others.$group });
    expect((await context.db.select(Items).where(Items.$group.notIn(groups))).results).toEqual([]);
    const present = context.db.select(Others, { group: Others.$group }).where(Others.$group.notEquals(null));
    expect(
      (await context.db.select(Items, { id: Items.$id }).where(Items.$group.notIn(present))).results.map(
        (row) => row.id,
      ),
    ).toEqual(["b"]);
  });

  // SQLAlchemy, test_insert.py
  it("inserts nothing from a query that gives no rows", async () => {
    const empty = context.db
      .select(Items, { id: Items.$id, group: Items.$group, count: Items.$count, parts: Items.$parts })
      .where(Items.$id.equals("missing"));
    await context.db.insert(Items).select(empty);
    await context.db.insert(Items).select(empty).onConflict(Items.$id).doNothing();
    expect(await ids()).toEqual([]);
  });

  // Kysely, insert.test.ts
  it.skipIf(engine === "mysql" || engine === "mariadb")(
    "returns every row of an upsert, inserted or updated, with its final values",
    async () => {
      await context.db.insert(Items, item("a", { count: 1 }));
      const upserted = await context.db
        .insert(Items, [item("a", { count: 10 }), item("b", { count: 20 })])
        .onConflict(Items.$id)
        .doUpdate({ count: add(Items.$count, incoming(Items.$count)) })
        .returning("id", "count");
      expect(upserted.results.sort((x, y) => x.id.localeCompare(y.id))).toEqual([
        { id: "a", count: 11 },
        { id: "b", count: 20 },
      ]);
    },
  );

  // SQLAlchemy, test_select.py JoinTest; Kysely, join.test.ts
  it("gives a missing row of a chain of left joins, and every row after it, as null", async () => {
    const Parents = createTable({
      id: "scenario_parent",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), itemId: z.string() }),
    });
    const Children = createTable({
      id: "scenario_child",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), parentId: z.string() }),
    });
    await context.db.syncTable(Parents);
    await context.db.syncTable(Children);
    await context.db.insert(Items, [item("a"), item("b")]);
    await context.db.insert(Parents, { id: "p", itemId: "a" });
    await context.db.insert(Children, { id: "c", parentId: "p" });
    const { results } = await context.db
      .select(Items, { id: Items.$id, parent: Parents, child: Children })
      .leftJoin(Parents, Parents.$itemId.equals(Items.$id))
      .leftJoin(Children, Children.$parentId.equals(Parents.$id))
      .orderBy(Items.$id, "ASC");
    expect(results).toEqual([
      { id: "a", parent: { id: "p", itemId: "a" }, child: { id: "c", parentId: "p" } },
      { id: "b", parent: null, child: null },
    ]);
    const unmatched = await context.db
      .select(Items, { id: Items.$id })
      .leftJoin(
        Parents,
        Parents.$itemId
          .equals(Items.$id)
          .and(
            notExists(context.db.select(Children, { id: Children.$id }).where(Children.$parentId.equals(Parents.$id))),
          ),
      )
      .where(Parents.$id.equals(null));
    expect(unmatched.results.map((row) => row.id).sort()).toEqual(["a", "b"]);
  });

  // Kysely, aggregate-function.test.ts; SQLAlchemy, test_group_by_composed
  it("counts the groups of a grouped query, and sums each", async () => {
    await context.db.insert(Items, [
      item("a", { group: "x", count: 1 }),
      item("b", { group: "x", count: 2 }),
      item("c", { group: "y", count: 5 }),
    ]);
    const grouped = context.db.select(Items, { group: Items.$group, total: sum(Items.$count) }).groupBy(Items.$group);
    expect(await grouped.clone().count()).toBe(2);
    expect((await grouped.orderBy(Items.$group, "ASC")).results).toEqual([
      { group: "x", total: 3 },
      { group: "y", total: 5 },
    ]);
  });

  // SQLAlchemy, test_cte.py test_select_recursive_round_trip
  it("follows a recursive query round a cycle once, as UNION drops the rows it has", async () => {
    const Edges = createTable({
      id: "scenario_edge",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), from: z.string(), to: z.string() }),
    });
    const Reached = createTable({ id: "reached", schema: z.object({ node: z.string() }) });
    await context.db.syncTable(Edges);
    await context.db.insert(Edges, [
      { id: "1", from: "a", to: "b" },
      { id: "2", from: "b", to: "c" },
      { id: "3", from: "c", to: "a" },
      { id: "4", from: "x", to: "y" },
    ]);
    const seed = context.db.select(Edges, { node: Edges.$from }).where(Edges.$from.equals("a"));
    const step = context.db.select(Edges, { node: Edges.$to }).innerJoin(Reached, Edges.$from.equals(Reached.$node));
    const reached = await context.db.withRecursive(Reached, union(seed, step)).select(Reached, { node: Reached.$node });
    expect(reached.results.map((row) => row.node).sort()).toEqual(["a", "b", "c"]);
  });

  // SQLAlchemy, test_select.py TrueDivTest
  it("divides one integer column by another exactly", async () => {
    await context.db.insert(Items, item("a", { count: 7, parts: 2 }));
    const { first } = await context.db.select(Items, { ratio: divide(Items.$count, Items.$parts) });
    expect(first?.ratio).toBe(3.5);
  });

  // SQLAlchemy, test_types.py JSONTest
  it("round trips large JSON with emoji, and tells a null inside it from a missing value", async () => {
    const text = `🐍 東京 ${"x".repeat(70_000)} 𝄞`;
    await context.db.insert(Items, [
      item("big", { data: { text, list: ["🐍", null] } }),
      item("nulls", { data: { text: "", list: [null] } }),
      item("none", { data: null }),
    ]);
    const stored = await context.db.select(Items, { id: Items.$id, data: Items.$data }).orderBy(Items.$id, "ASC");
    expect(stored.results).toEqual([
      { id: "big", data: { text, list: ["🐍", null] } },
      { id: "none", data: null },
      { id: "nulls", data: { text: "", list: [null] } },
    ]);
    expect(
      (await context.db.select(Items, { id: Items.$id }).where(Items.$data.equals(null))).results.map((row) => row.id),
    ).toEqual(["none"]);
  });
});
