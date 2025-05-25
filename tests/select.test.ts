import BunDatabase from "bun:sqlite";
import * as zod from "zod/v4";
import { createTable, Database } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

it("select only field", async () => {
  const BoardTable = createTable({
    id: "board",
    schema: zod.object({
      id: zod.string(),
      otherField: zod.string(),
    }),
  });
  const rawDb = new BunDatabase(":memory:");
  const db = new Database({
    adaptor: new BunSqliteAdaptor({
      driver: rawDb
    }),
  });

  await db.syncTable(BoardTable);

  const items = [
    { id: crypto.randomUUID(), otherField: "a" },
    { id: crypto.randomUUID(), otherField: "b" },
    { id: crypto.randomUUID(), otherField: "c" },
    { id: crypto.randomUUID(), otherField: "d" },
  ];
  await db.insertMany(BoardTable, items);

  const { results } = await db.select(BoardTable, ["id"]);

  expect(Array.from(results)).toEqual(items.map(({ id }) => ({ id })));
});
