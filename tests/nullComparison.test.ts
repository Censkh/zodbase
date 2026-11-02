import BunDatabase from "bun:sqlite";
import { describe, expect, it, spyOn } from "bun:test";
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
  it("preserves nested AND/OR grouping and validates every branch", async () => {
    const db = await createDb();
    await db.insert(ItemTable, { id: "excluded", folderId: "f1", name: "excluded", credits: 3 });
    const condition = ItemTable.$id
      .equals("root")
      .or(ItemTable.$folderId.equals("f1").and(ItemTable.$credits.greaterThan(6)));
    expect(
      (await db.select(ItemTable).where(condition).orderBy(ItemTable.$id, "ASC")).results.map((row) => row.id),
    ).toEqual(["child", "root"]);
    const invalid = ItemTable.$id
      .equals("root")
      .or(ItemTable.$id.equals("child").and({ type: "XOR", conditions: [] } as any));
    expect(() => db.select(ItemTable).where(invalid).clone()).toThrow(Error);
    const execute = spyOn(db.adaptor, "execute");
    try {
      await expect(Promise.resolve(db.delete(ItemTable).where(invalid))).rejects.toBeInstanceOf(Error);
      expect(execute).not.toHaveBeenCalled();
    } finally {
      execute.mockRestore();
    }
    expect((await db.select(ItemTable).orderBy(ItemTable.$id, "ASC")).results.map((row) => row.id)).toEqual([
      "child",
      "excluded",
      "root",
    ]);
  });

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
      await expect(Promise.resolve(db.select(ItemTable).limit(value))).rejects.toThrow("Invalid limit");
      await expect(Promise.resolve(db.select(ItemTable).offset(value))).rejects.toThrow("Invalid offset");
    }
    const { results } = await db.select(ItemTable).limit(1).offset(1);
    expect(results).toHaveLength(1);
  });

  it("rejects an order direction other than ASC or DESC", async () => {
    const db = await createDb();
    await expect(
      Promise.resolve(db.select(ItemTable).orderBy(ItemTable.$name, "ASC; DROP TABLE item" as any)),
    ).rejects.toThrow("Invalid order direction");
  });
});
