import BunDatabase from "bun:sqlite";
import * as zod from "zod/v4";
import { createTable, Database } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

describe("tableDiff", () => {
  it("test adding new fields", async () => {
    const table = createTable({
      id: "user",
      schema: zod.object({
        id: zod.string(),
        name: zod.string(),
      }),
    });

    const rawDb = new BunDatabase(":memory:");
    const db = new Database({
      adaptor: new BunSqliteAdaptor({
        driver: rawDb,
      }),
    });

    await db.syncTable(table);

    const result = await rawDb.query("PRAGMA table_info(user)").all();
    expect(result).toMatchObject([
      {
        name: "id",
      },
      {
        name: "name",
      },
    ]);

    const updatedTable = createTable({
      id: "user",
      schema: zod.object({
        id: zod.string(),
        name: zod.string(),
        age: zod.number(),
      }),
    });

    await db.syncTable(updatedTable);

    const updatedResult = await rawDb.query("PRAGMA table_info(user)").all();

    expect(updatedResult).toMatchObject([
      {
        name: "id",
      },
      {
        name: "name",
      },
      {
        name: "age",
      },
    ]);
  });
});
