import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database, metaStore, primaryKey } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

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
});
