import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database, exists, incoming, metaStore, monotonicTimestamp, primaryKey, values } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";
import PostgresAdaptor from "../src/adaptors/postgres";

describe("lazy adaptor initialization", () => {
  it("initializes once when the first database operation executes", async () => {
    const driver = new BunDatabase(":memory:");
    let initializationCount = 0;
    const database = new Database({
      adaptor: async () => {
        initializationCount += 1;
        return new BunSqliteAdaptor({ driver });
      },
    });
    const AssetsTable = createTable({
      id: "lazy_assets",
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        tags: zod.array(zod.string()),
      }),
    });

    const query = database.select(AssetsTable, { id: AssetsTable.$id }).where(AssetsTable.$tags.contains("cached"));
    expect(initializationCount).toBe(0);

    await database.syncTable(AssetsTable);
    await database.insert(AssetsTable, { id: "asset-1", tags: ["cached"] });
    const [firstResult, secondResult] = await Promise.all([query, database.select(AssetsTable).count()]);

    expect(initializationCount).toBe(1);
    expect(firstResult.results).toEqual([{ id: "asset-1" }]);
    expect(secondResult).toBe(1);
    driver.close();
  });
  it("builds every kind of write with the adaptor it initializes", async () => {
    const driver = new BunDatabase(":memory:");
    const database = new Database({ adaptor: async () => new BunSqliteAdaptor({ driver }) });
    const Items = createTable({
      id: "lazy_items",
      schema: zod.object({ id: zod.string().meta(metaStore([primaryKey()])), count: zod.number() }),
    });
    const Copies = createTable({
      id: "lazy_copies",
      schema: zod.object({ id: zod.string().meta(metaStore([primaryKey()])), count: zod.number() }),
    });
    await database.syncTable(Items);
    await database.syncTable(Copies);
    await database.insert(Items, { id: "a", count: 1 });

    // A conditional insert, kept only while its condition holds.
    const present = exists(database.select(Items, { id: Items.$id }).where(Items.$id.equals("a")));
    const absent = exists(database.select(Items, { id: Items.$id }).where(Items.$id.equals("missing")));
    await database.insert(Copies, [{ id: "kept", count: 1 }]).where(present);
    await database.insert(Copies, [{ id: "dropped", count: 1 }]).where(absent);
    // An insert from a query, merging into a row that exists.
    await database
      .insert(Copies)
      .select(database.select(Items, { id: Items.$id, count: Items.$count }))
      .onConflict(Copies.$id)
      .doUpdate({ count: incoming(Copies.$count) });
    await database
      .insert(Copies)
      .select(database.select(Items, { id: Items.$id, count: Items.$count }))
      .onConflict(Copies.$id)
      .doUpdate({ count: incoming(Copies.$count) });
    // An insert from literal rows read as a table.
    const Rows = createTable({ id: "lazy_rows", schema: Copies.schema });
    const rows = values(Rows, [{ id: "v", count: 5 }]);
    await database.insert(Copies).select(database.select(rows, { id: rows.$id, count: rows.$count }));

    const stored = await database.select(Copies, { id: Copies.$id, count: Copies.$count }).orderBy(Copies.$id, "ASC");
    expect(stored.results).toEqual([
      { id: "a", count: 1 },
      { id: "kept", count: 1 },
      { id: "v", count: 5 },
    ]);
    driver.close();
  });
  it("runs a write after a WITH clause with the adaptor it initializes", async () => {
    const statements: string[] = [];
    const driver = {
      async query(statement: string) {
        statements.push(statement);
        return { rows: [] };
      },
    };
    const database = new Database({ adaptor: async () => new PostgresAdaptor({ driver: driver as any }) });
    const Items = createTable({
      id: "lazy_with_items",
      schema: zod.object({ id: zod.string().meta(metaStore([primaryKey()])), count: zod.number() }),
    });
    const Removed = createTable({ id: "removed", schema: zod.object({ id: zod.string(), count: zod.number() }) });
    const removed = database.delete(Items).where(Items.$count.equals(0)).returning("id", "count");
    await database
      .with(Removed, removed)
      .insert(Items)
      .select(database.select(Removed, { id: Removed.$id, count: Removed.$count }));
    expect(statements).toHaveLength(1);
    expect(statements[0]!.replace(/\s+/g, " ")).toStartWith('WITH "removed" AS (DELETE FROM "lazy_with_items"');
  });
  it("resolves dialect helpers for migrations and revision writes", async () => {
    const statements: string[] = [];
    const driver = {
      async query(statement: string) {
        statements.push(statement);
        if (statement.includes("information_schema.columns"))
          return {
            rows: [
              {
                column_name: "updatedAt",
                data_type: "real",
                is_nullable: "NO",
                column_default: null,
                is_identity: "NO",
                is_primary_key: false,
              },
            ],
          };
        if (statement.includes("SELECT MAX")) return { rows: [{ revision: 1789021300000 }] };
        return { rows: [] };
      },
    };
    const database = new Database({ adaptor: async () => new PostgresAdaptor({ driver: driver as any }) });
    const table = createTable({
      id: "lazy_revisions",
      schema: zod.object({ updatedAt: zod.number().meta(metaStore([monotonicTimestamp()])) }),
    });
    // Constructing the write must not require initialization of the async adaptor.
    const update = database.update(table, { updatedAt: 0 }).where(table.$updatedAt.greaterThan(0));
    expect(statements).toEqual([]);
    await database.syncTable(table);
    await update;
    expect(statements.some((sql) => sql.includes('ALTER COLUMN "updatedAt" TYPE DOUBLE PRECISION'))).toBe(true);
    expect(statements.at(-1)).toContain('CASE WHEN "updatedAt"');
  });
});
