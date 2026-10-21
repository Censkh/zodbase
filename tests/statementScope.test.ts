import { expect, test } from "bun:test";
import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database, sql } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

test("filters and paginates membership in a recursive SQL scope", async () => {
  const driver = new BunDatabase(":memory:");
  const db = new Database({ adaptor: new BunSqliteAdaptor({ driver }) });
  const table = createTable({
    id: "scope_rows",
    schema: zod.object({ id: zod.string(), parentId: zod.string().nullable() }),
  });
  try {
    await db.syncTable(table);
    await db.insertMany(table, [
      { id: "root", parentId: null },
      { id: "child", parentId: "root" },
      { id: "grandchild", parentId: "child" },
      { id: "outside", parentId: null },
    ]);
    const scope = sql`(WITH RECURSIVE subtree AS (SELECT id FROM scope_rows WHERE id = ${"root"} UNION SELECT child.id FROM scope_rows child JOIN subtree parent ON child.parentId = parent.id) SELECT id FROM subtree)`;
    const page = await db.select(table).where(table.$id.in(scope)).orderBy(table.$id, "ASC").offset(1).limit(2);
    expect(page.results.map((row) => row.id)).toEqual(["grandchild", "root"]);
    const outside = await db.select(table).where(table.$id.notIn(scope));
    expect(outside.results.map((row) => row.id)).toEqual(["outside"]);
  } finally {
    driver.close();
  }
});
