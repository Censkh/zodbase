import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as zod from "zod";
import { createTable, Database, metaStore, primaryKey } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const createItems = () =>
  createTable({
    id: "load_item",
    schema: zod.object({
      id: zod.string().meta(metaStore([primaryKey()])),
      group: zod.string(),
    }),
  });

const items = [
  { id: "a", group: "x" },
  { id: "b", group: "x" },
  { id: "c", group: "y" },
];

describe.each(TEST_DATABASE_FACTORIES)("load: $name ($connection)", ({ create }) => {
  let context: TestDatabaseContext;
  let db: Database;
  let Items: ReturnType<typeof createItems>;

  beforeEach(async () => {
    context = await create();
    db = context.db as Database;
    Items = createItems();
    await db.syncTable(Items);
    await db.insert(Items, items);
  });

  afterEach(async () => {
    await context.close();
  });

  it("reads the rows loaded together in one statement, each resolving to its own", async () => {
    const select = spyOn(Database.prototype, "select");
    try {
      const loaded = await Promise.all([
        db.load(Items.$id, "a"),
        db.load(Items.$id, "c"),
        db.load(Items.$id, "a"),
        db.load(Items.$id, "missing"),
      ]);
      expect(loaded).toEqual([items[0], items[2], items[0], undefined]);
      expect(select).toHaveBeenCalledTimes(1);
    } finally {
      select.mockRestore();
    }
  });

  it("reads loads under different conditions, or started later, apart", async () => {
    const select = spyOn(Database.prototype, "select");
    try {
      const [inX, inY] = await Promise.all([
        db.load(Items.$id, "a", { where: Items.$group.equals("x") }),
        db.load(Items.$id, "a", { where: Items.$group.equals("y") }),
      ]);
      expect(inX).toEqual(items[0]);
      expect(inY).toBeUndefined();
      expect(select).toHaveBeenCalledTimes(2);
      expect(await db.load(Items.$id, "b")).toEqual(items[1]);
      expect(select).toHaveBeenCalledTimes(3);
    } finally {
      select.mockRestore();
    }
  });

  it("rejects every load of a statement that fails", async () => {
    const Missing = createTable({
      id: "load_missing",
      schema: zod.object({ id: zod.string().meta(metaStore([primaryKey()])) }),
    });
    const loads = [db.load(Missing.$id, "a"), db.load(Missing.$id, "b")];
    const settled = await Promise.allSettled(loads);
    expect(settled.map((result) => result.status)).toEqual(["rejected", "rejected"]);
  });
});
