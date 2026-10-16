import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database, metaStore, primaryKey } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

const ItemTable = createTable({
  id: "item",
  schema: zod.object({
    id: zod.string().meta(metaStore([primaryKey()])),
    folderId: zod.string().nullable(),
    name: zod.string(),
    credits: zod.number().default(100),
  }),
});

const createDb = async () => {
  const db = new Database({ adaptor: new BunSqliteAdaptor({ driver: new BunDatabase(":memory:") }) });
  await db.syncTable(ItemTable);
  await db.insertMany(ItemTable, [
    { id: "root", folderId: null, name: "root", credits: 5 },
    { id: "child", folderId: "f1", name: "child", credits: 7 },
  ]);
  return db;
};

describe("comparisons with a missing value", () => {
  it("treats equals(undefined) as IS NULL", async () => {
    const db = await createDb();
    const folderId: string | null | undefined = undefined;
    const { results } = await db.select(ItemTable).where(ItemTable.$folderId.equals(folderId as any));
    expect(results.map((row) => row.id)).toEqual(["root"]);
  });

  it("treats notEquals(undefined) as IS NOT NULL", async () => {
    const db = await createDb();
    const { results } = await db.select(ItemTable).where(ItemTable.$folderId.notEquals(undefined));
    expect(results.map((row) => row.id)).toEqual(["child"]);
  });
});

describe("paging and ordering arguments", () => {
  it("rejects limits and offsets that are not non-negative safe integers", async () => {
    const db = await createDb();
    for (const value of [-1, 1.5, Number.NaN, 1e21, "5" as unknown as number]) {
      expect(() => db.select(ItemTable).limit(value)).toThrow("Invalid limit");
      expect(() => db.select(ItemTable).offset(value)).toThrow("Invalid offset");
    }
    const { results } = await db.select(ItemTable).limit(1).offset(1);
    expect(results).toHaveLength(1);
  });

  it("rejects an order direction other than ASC or DESC", async () => {
    const db = await createDb();
    expect(() => db.select(ItemTable).orderBy(ItemTable.$name, "ASC; DROP TABLE item" as any)).toThrow(
      "Invalid order direction",
    );
  });
});
