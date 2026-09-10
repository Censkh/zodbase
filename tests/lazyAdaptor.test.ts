import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database, metaStore, monotonicTimestamp, primaryKey } from "../src";
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

    const query = database.select(AssetsTable, ["id"]).where(AssetsTable.$tags.contains("cached"));
    expect(initializationCount).toBe(0);

    await database.syncTable(AssetsTable);
    await database.insert(AssetsTable, { id: "asset-1", tags: ["cached"] });
    const [firstResult, secondResult] = await Promise.all([query, database.count(AssetsTable)]);

    expect(initializationCount).toBe(1);
    expect(firstResult.results).toEqual([{ id: "asset-1" }]);
    expect(secondResult.first).toEqual({ _count: 1 });
    driver.close();
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
    const update = database.update(table, { updatedAt: 0 }, table.$updatedAt.greaterThan(0));
    expect(statements).toEqual([]);
    await database.syncTable(table);
    await update;
    expect(statements.some((sql) => sql.includes('ALTER COLUMN "updatedAt" TYPE DOUBLE PRECISION'))).toBe(true);
    expect(statements.at(-1)).toContain('CASE WHEN "updatedAt"');
  });
});
