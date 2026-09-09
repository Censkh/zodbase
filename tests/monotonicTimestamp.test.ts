import BunDatabase from "bun:sqlite";
import { expect, test } from "bun:test";
import * as zod from "zod";
import { createTable, Database, metaStore, monotonicTimestamp, primaryKey } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

test("concurrent writes advance versions even when the stored clock is ahead", async () => {
  const driver = new BunDatabase(":memory:");
  try {
    const db = new Database({ adaptor: new BunSqliteAdaptor({ driver }) });
    const table = createTable({
      id: "versioned",
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        name: zod.string(),
        updatedAt: zod.number().meta(metaStore([monotonicTimestamp()])),
      }),
    });
    await db.syncTable(table);
    const future = Date.now() + 100_000;
    await db.insert(table, { id: "one", name: "initial", updatedAt: future });
    const results = await Promise.all(
      ["a", "b", "c"].map((name) => db.update(table, { name, updatedAt: 1 }, table.$id.equals("one")).selectMutated()),
    );
    expect(results.map((result) => result.first.updatedAt)).toEqual([future + 1, future + 2, future + 3]);
    expect((await db.select(table, ["*"])).first?.name).toBe("c");
  } finally {
    driver.close();
  }
});
