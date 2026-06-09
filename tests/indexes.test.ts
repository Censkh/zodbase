import BunDatabase from "bun:sqlite";
import * as zod from "zod/v4";
import { createTable, Database } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

const createTestDatabase = () => {
  const rawDb = new BunDatabase(":memory:");
  const db = new Database({
    adaptor: new BunSqliteAdaptor({
      driver: rawDb,
    }),
  });

  return { db, rawDb };
};

const createAssetTable = () =>
  createTable({
    id: "asset",
    schema: zod.object({
      id: zod.string(),
      projectId: zod.string(),
      externalId: zod.string().nullish(),
      status: zod.string(),
    }),
  });

describe("indexes", () => {
  it("creates indexes added with table field bindings and condition builder predicates", async () => {
    const AssetTable = createAssetTable();
    AssetTable.addIndex("asset_project_external_id_unique", [AssetTable.$projectId, AssetTable.$externalId], {
      unique: true,
      where: AssetTable.$externalId.notEquals(null),
    });

    const { db, rawDb } = createTestDatabase();
    await db.syncTable(AssetTable);

    expect(rawDb.query("PRAGMA index_list(asset)").all()).toContainEqual(
      expect.objectContaining({
        name: "asset_project_external_id_unique",
        unique: 1,
        partial: 1,
      }),
    );
    expect(rawDb.query("PRAGMA index_info(asset_project_external_id_unique)").all()).toMatchObject([
      { name: "projectId" },
      { name: "externalId" },
    ]);
    expect(
      rawDb
        .query("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'asset_project_external_id_unique'")
        .get(),
    ).toMatchObject({
      sql: expect.stringContaining("WHERE externalId IS NOT NULL"),
    });
  });

  it("creates indexes added after the table already exists", async () => {
    const { db, rawDb } = createTestDatabase();

    await db.syncTable(createAssetTable());
    expect(rawDb.query("PRAGMA index_list(asset)").all()).not.toContainEqual(
      expect.objectContaining({ name: "asset_project_external_id_unique" }),
    );

    const AssetTableWithIndex = createAssetTable();
    AssetTableWithIndex.addIndex(
      "asset_project_external_id_unique",
      [AssetTableWithIndex.$projectId, AssetTableWithIndex.$externalId],
      {
        unique: true,
        where: AssetTableWithIndex.$externalId.notEquals(null),
      },
    );

    await db.syncTable(AssetTableWithIndex);

    expect(rawDb.query("PRAGMA index_list(asset)").all()).toContainEqual(
      expect.objectContaining({ name: "asset_project_external_id_unique" }),
    );
  });

  it("can sync the same index repeatedly", async () => {
    const AssetTable = createAssetTable();
    AssetTable.addIndex("asset_status_index", [AssetTable.$status]);

    const { db, rawDb } = createTestDatabase();
    await db.syncTable(AssetTable);
    await db.syncTable(AssetTable);

    expect(
      rawDb
        .query("SELECT COUNT(*) as count FROM sqlite_master WHERE type = 'index' AND name = 'asset_status_index'")
        .get(),
    ).toMatchObject({ count: 1 });
  });

  it("enforces partial unique indexes without rejecting null values", async () => {
    const AssetTable = createAssetTable();
    AssetTable.addIndex("asset_project_external_id_unique", [AssetTable.$projectId, AssetTable.$externalId], {
      unique: true,
      where: AssetTable.$externalId.notEquals(null),
    });

    const { db } = createTestDatabase();
    await db.syncTable(AssetTable);

    await db.insert(AssetTable, {
      id: "asset-1",
      projectId: "project-1",
      externalId: "external-1",
      status: "pending",
    });
    let duplicateError: unknown;
    try {
      await db.insert(AssetTable, {
        id: "asset-2",
        projectId: "project-1",
        externalId: "external-1",
        status: "pending",
      });
    } catch (error) {
      duplicateError = error;
    }
    expect(duplicateError).toBeDefined();
    await db.insert(AssetTable, {
      id: "asset-3",
      projectId: "project-2",
      externalId: "external-1",
      status: "pending",
    });
    await db.insert(AssetTable, {
      id: "asset-4",
      projectId: "project-1",
      externalId: null,
      status: "pending",
    });
    await db.insert(AssetTable, {
      id: "asset-5",
      projectId: "project-1",
      externalId: null,
      status: "pending",
    });
  });

  it("supports compound partial index predicates", async () => {
    const AssetTable = createAssetTable();
    AssetTable.addIndex("asset_project_external_pending_unique", [AssetTable.$projectId, AssetTable.$externalId], {
      unique: true,
      where: AssetTable.$externalId.notEquals(null).and(AssetTable.$status.equals("pending")),
    });

    const { db, rawDb } = createTestDatabase();
    await db.syncTable(AssetTable);

    const indexSql = rawDb
      .query("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'asset_project_external_pending_unique'")
      .get() as { sql: string };

    expect(indexSql.sql.replace(/\s+/g, " ")).toContain("(externalId IS NOT NULL AND status = 'pending')");
  });
});
