import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database, lazyTable } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

const createTestDatabase = () => {
  const rawDb = new BunDatabase(":memory:");
  const db = new Database({ adaptor: new BunSqliteAdaptor({ driver: rawDb }) });
  return { db, rawDb };
};

describe("lazyTable", () => {
  it("defines the table on first use, with the indexes its definition adds", async () => {
    let defined = 0;
    const AssetTable = lazyTable(() => {
      defined++;
      const table = createTable({
        id: "asset",
        schema: zod.object({ id: zod.string(), projectId: zod.string(), name: zod.string() }),
      });
      table.addIndex("asset_project", [table.$projectId]);
      return table;
    });
    expect(defined).toBe(0);

    const { db, rawDb } = createTestDatabase();
    await db.syncTable(AssetTable);
    expect(defined).toBe(1);
    expect(rawDb.query("PRAGMA index_list(asset)").all()).toContainEqual(
      expect.objectContaining({ name: "asset_project" }),
    );

    await db.insert(AssetTable, { id: "a1", projectId: "p1", name: "first" });
    const { first } = await db.select(AssetTable).where(AssetTable.$projectId.equals("p1")).one();
    expect(first).toEqual({ id: "a1", projectId: "p1", name: "first" });
    expect(defined).toBe(1);
  });

  it("binds its fields to the table its importers hold", () => {
    const AssetTable = lazyTable(() => createTable({ id: "asset", schema: zod.object({ id: zod.string() }) }));
    expect(AssetTable.$id.table).toBe(AssetTable);
    expect(AssetTable.fields.id.table).toBe(AssetTable);
  });

  it("builds a table its definition reads without giving it its own", () => {
    const ProjectTable = lazyTable(() => createTable({ id: "project", schema: zod.object({ id: zod.string() }) }));
    const AssetTable = lazyTable(() => {
      // Reading another lazy table builds it in the middle of this definition.
      const projectKey = ProjectTable.$id;
      const table = createTable({
        id: "asset",
        schema: zod.object({ id: zod.string(), projectId: projectKey.schema }),
      });
      return table;
    });
    expect(String(AssetTable.id)).toBe("asset");
    expect(String(ProjectTable.id)).toBe("project");
    expect(AssetTable.$projectId.table).toBe(AssetTable);
    expect(ProjectTable.$id.table).toBe(ProjectTable);
  });

  it("refuses a definition that returns a table other than the one it created", () => {
    const OtherTable = createTable({ id: "other", schema: zod.object({ id: zod.string() }) });
    const AssetTable = lazyTable(() => {
      createTable({ id: "asset", schema: zod.object({ id: zod.string() }) });
      return OtherTable as any;
    });
    expect(() => AssetTable.id).toThrow("A lazyTable definition must return the table its createTable made");
  });
});
