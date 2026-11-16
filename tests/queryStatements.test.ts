import { Database as Sqlite } from "bun:sqlite";
import { expect, test } from "bun:test";
import * as z from "zod";
import { createTable, Database, exists, union, withCtes } from "../src";
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
    await db.insert(Tree, [
      { id: 1, parentId: null, name: "Parent *%_!" },
      { id: 2, parentId: 1, name: "Child" },
      { id: 3, parentId: 2, name: "Grandchild" },
      { id: 4, parentId: null, name: "Parent *aaaZ!" },
    ]);
    const seed = db.select(Tree, { id: Tree.$id }).where(Tree.$name.containsText("PARENT *%_!"));
    // A select is checked when it becomes a statement, as one that can't run never compiles.
    expect(() => union(seed.clone().limit(-1), seed)).toThrow("Invalid limit");
    const children = db.select(Tree, { id: Tree.$id }).innerJoin(Branch, Tree.$parentId.equals(Branch.$id));
    const branches = withCtes(
      [{ table: Branch, query: union(seed, children) }],
      db.select(Branch, { id: Branch.$id }),
      { recursive: true },
    );
    const ids = await db.select(Tree, { id: Tree.$id }).where(Tree.$id.in(branches)).orderBy(Tree.$id, "ASC");
    expect(ids.results).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    const missing = db.select(Tree, { id: Tree.$id }).where(Tree.$id.equals(0));
    expect((await db.select(Tree).where(Tree.$id.in(branches).and(exists(missing)))).results).toEqual([]);
  } finally {
    driver.close();
  }
});
