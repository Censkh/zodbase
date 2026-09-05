import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database, metaStore, primaryKey, updatedAt } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

describe("partial update defaults", () => {
  let driver: BunDatabase;
  let database: Database;
  let defaultCalls: number;
  const makeTable = () =>
    createTable({
      id: "update_defaults",
      schema: zod.object({
        id: zod
          .string()
          .default(() => {
            defaultCalls += 1;
            return crypto.randomUUID();
          })
          .meta(metaStore([primaryKey()])),
        name: zod.string().trim(),
        credits: zod.number().default(100),
        note: zod.string().nullable().default("initial"),
        changedAt: zod
          .number()
          .optional()
          .meta(metaStore([updatedAt()])),
      }),
    });
  let table: ReturnType<typeof makeTable>;

  beforeEach(async () => {
    driver = new BunDatabase(":memory:");
    database = new Database({ adaptor: new BunSqliteAdaptor({ driver }) });
    defaultCalls = 0;
    table = makeTable();
    await database.syncTable(table);
    await database.insertMany(table, [
      { id: "first", name: "First", credits: 25, note: "kept" },
      { id: "second", name: "Second", credits: 50, note: "also kept" },
    ]);
  });
  afterEach(() => driver.close());

  it("preserves omitted primary keys and defaults while validating supplied fields", async () => {
    const result = await database.update(table, { name: "  Changed  " }, table.$id.equals("first")).selectMutated();
    expect(result.first).toMatchObject({ id: "first", name: "Changed", credits: 25, note: "kept" });
    expect(result.first.changedAt).toBeGreaterThan(0);
    expect(defaultCalls).toBe(0);
  });

  it("does not replace omitted defaults in ordinary updates", async () => {
    await database.update(table, { credits: 10 }, table.$id.equals("first"));
    expect((await database.select(table, ["*"]).where(table.$id.equals("first"))).first).toMatchObject({
      id: "first",
      credits: 10,
      note: "kept",
    });
    expect(defaultCalls).toBe(0);
  });

  it("preserves omitted values separately for each bulk update", async () => {
    await database
      .updateMany(
        table,
        [
          { id: "first", name: "  Updated  " },
          { id: "second", credits: 40 },
        ],
        table.$id,
      )
      .selectMutated();
    expect((await database.select(table, ["*"]).orderBy(table.$id, "ASC")).results).toMatchObject([
      { id: "first", name: "Updated", credits: 25, note: "kept" },
      { id: "second", name: "Second", credits: 40, note: "also kept" },
    ]);
  });

  it("applies defaults only when explicitly requested and retains explicit nulls", async () => {
    await database.update(table, { credits: undefined, note: null }, table.$id.equals("first"));
    expect((await database.select(table, ["*"]).where(table.$id.equals("first"))).first).toMatchObject({
      id: "first",
      credits: 100,
      note: null,
    });
    expect(defaultCalls).toBe(0);
  });

  it("rejects invalid supplied fields before writing", async () => {
    expect(() => database.update(table, { credits: "bad" } as any, table.$id.equals("first"))).toThrow();
    expect((await database.select(table, ["*"]).where(table.$id.equals("first"))).first?.credits).toBe(25);
  });
});
