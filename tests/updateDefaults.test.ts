import * as zod from "zod";
import { createTable, type Database, metaStore, primaryKey, updatedAt } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

describe.each(TEST_DATABASE_FACTORIES)("partial update defaults: $name ($connection)", ({ create }) => {
  let context: TestDatabaseContext;
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
    context = await create();
    database = context.db;
    defaultCalls = 0;
    table = makeTable();
    await database.syncTable(table);
    await database.insert(table, [
      { id: "first", name: "First", credits: 25, note: "kept" },
      { id: "second", name: "Second", credits: 50, note: "also kept" },
    ]);
    defaultCalls = 0;
  });
  afterEach(async () => context.close());

  it("preserves omitted primary keys and defaults while validating supplied fields", async () => {
    const result = await database.update(table, { name: "  Changed  " }).where(table.$id.equals("first")).returning();
    expect(result.first).toMatchObject({ id: "first", name: "Changed", credits: 25, note: "kept" });
    expect(result.first.changedAt).toBeGreaterThan(0);
    expect(defaultCalls).toBe(0);
  });

  it("keeps an update's own change time, or the stored one it names, rather than stamping", async () => {
    await database.update(table, { name: "Set", changedAt: 7 }).where(table.$id.equals("first"));
    await database.update(table, { name: "Kept", changedAt: table.$changedAt }).where(table.$id.equals("first"));
    expect((await database.select(table).where(table.$id.equals("first"))).first).toMatchObject({
      name: "Kept",
      changedAt: 7,
    });
  });

  it("does not replace omitted defaults in ordinary updates", async () => {
    await database.update(table, { credits: 10 }).where(table.$id.equals("first"));
    expect((await database.select(table).where(table.$id.equals("first"))).first).toMatchObject({
      id: "first",
      credits: 10,
      note: "kept",
    });
    expect(defaultCalls).toBe(0);
  });

  it("preserves omitted values separately for each bulk update", async () => {
    await database
      .update(table, [
        { id: "first", name: "  Updated  " },
        { id: "second", credits: 40 },
      ])
      .by(table.$id)
      .returning();
    expect((await database.select(table).orderBy(table.$id, "ASC")).results).toMatchObject([
      { id: "first", name: "Updated", credits: 25, note: "kept" },
      { id: "second", name: "Second", credits: 40, note: "also kept" },
    ]);
  });

  it("applies defaults only when explicitly requested and retains explicit nulls", async () => {
    await database.update(table, { credits: undefined, note: null }).where(table.$id.equals("first"));
    expect((await database.select(table).where(table.$id.equals("first"))).first).toMatchObject({
      id: "first",
      credits: 100,
      note: null,
    });
    expect(defaultCalls).toBe(0);
  });

  it("rejects invalid supplied fields before writing", async () => {
    expect(() => database.update(table, { credits: "bad" } as any).where(table.$id.equals("first"))).toThrow();
    expect((await database.select(table).where(table.$id.equals("first"))).first?.credits).toBe(25);
  });
});
