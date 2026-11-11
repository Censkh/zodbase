import { Database as Sqlite } from "bun:sqlite";
import { expect, test } from "bun:test";
import * as z from "zod";
import {
  coalesce,
  createTable,
  Database,
  exists,
  expression,
  selectStatement,
  sql,
  sum,
  union,
  withCtes,
} from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

test("composes recursive statements with named projections and literal text predicates", async () => {
  const driver = new Sqlite(":memory:");
  const db = new Database({ adaptor: async () => new BunSqliteAdaptor({ driver }) });
  const Tree = createTable({
    id: "tree",
    schema: z.object({ id: z.number(), parentId: z.number().nullable(), name: z.string() }),
  });
  const Branch = createTable({ id: "branch", schema: Tree.schema.pick({ id: true }) });
  try {
    await db.syncTable(Tree);
    await db.insertMany(Tree, [
      { id: 1, parentId: null, name: "Parent *%_!" },
      { id: 2, parentId: 1, name: "Child" },
      { id: 3, parentId: 2, name: "Grandchild" },
      { id: 4, parentId: null, name: "Parent *aaaZ!" },
    ]);
    const seed = db.select(Tree, { id: Tree.$id }).where(Tree.$name.containsText("PARENT *%_!"));
    expect(() => selectStatement(seed.clone().limit(-1))).toThrow("Invalid limit");
    const children = db.select(Tree, { id: Tree.$id }).innerJoin(Branch, Tree.$parentId.equals(Branch.$id));
    const branches = withCtes(
      [{ table: Branch, query: union(selectStatement(seed), selectStatement(children)) }],
      selectStatement(db.select(Branch, { id: Branch.$id })),
      { recursive: true },
    );
    const ids = await db.select(Tree, { id: Tree.$id }).where(Tree.$id.in(branches)).orderBy(Tree.$id, "ASC");
    expect(ids.results).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    const missing = selectStatement(db.select(Tree, { id: Tree.$id }).where(Tree.$id.equals(0)));
    expect((await db.select(Tree).where(Tree.$id.in(branches).and(exists(missing)))).results).toEqual([]);
  } finally {
    driver.close();
  }
});

test("updates an existing value by a scalar aggregate in the database", async () => {
  const driver = new Sqlite(":memory:");
  const db = new Database({ adaptor: new BunSqliteAdaptor({ driver }) });
  const Totals = createTable({ id: "totals", schema: z.object({ id: z.string(), amount: z.number() }) });
  const Deductions = createTable({ id: "deductions", schema: z.object({ owner: z.string(), amount: z.number() }) });
  try {
    await db.syncTable(Totals);
    await db.syncTable(Deductions);
    await db.insertMany(Totals, [
      { id: "a", amount: 9 },
      { id: "b", amount: 2 },
    ]);
    await db.insertMany(Deductions, [
      { owner: "a", amount: 3 },
      { owner: "b", amount: 2 },
    ]);
    const subtotal = await db.compile(
      selectStatement(
        db
          .select(Deductions, {
            amount: coalesce(sum(Deductions.$amount), 0),
          })
          .where(Deductions.$owner.equals("a")),
      ),
    );
    const empty = await db
      .select(Deductions, { amount: coalesce(sum(Deductions.$amount), 0) })
      .where(Deductions.$owner.equals("missing"));
    expect(empty.first?.amount).toBe(0);
    await db.update(
      Totals,
      {
        amount: expression(sql`${Totals.$amount} - (${subtotal})`, z.number()),
      },
      Totals.$id.equals("a"),
    );
    expect((await db.select(Totals).orderBy(Totals.$id, "ASC")).results).toEqual([
      { id: "a", amount: 6 },
      { id: "b", amount: 2 },
    ]);
  } finally {
    driver.close();
  }
});
