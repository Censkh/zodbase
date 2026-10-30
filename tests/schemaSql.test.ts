import BunDatabase from "bun:sqlite";
import { expect, test } from "bun:test";
import * as zod from "zod";
import { createTable, Database, foreignKey, metaStore, primaryKey } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

const createTables = () => {
  const Project = createTable({
    id: "project",
    schema: zod.object({ id: zod.string().meta(metaStore([primaryKey()])), name: zod.string() }),
  });
  const Asset = createTable({
    id: "asset",
    schema: zod.object({
      id: zod.string().meta(metaStore([primaryKey()])),
      projectId: zod.string().meta(metaStore([foreignKey({ field: Project.$id, onDelete: "cascade" })])),
      externalId: zod.string().nullish(),
      size: zod.number(),
    }),
  });
  Asset.addIndex("asset_project_external_id_unique", [Asset.$projectId, Asset.$externalId], {
    unique: true,
    where: Asset.$externalId.notEquals(null),
  });
  Asset.addIndex("asset_project", [Asset.$projectId]);
  return [Project, Asset];
};

// SQLite keeps each statement's text as written, so whitespace is collapsed before comparing.
const schemaOf = (raw: BunDatabase) =>
  raw
    .query<{ name: string; sql: string | null }, []>(
      "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name",
    )
    .all()
    .map((row) => ({ ...row, sql: row.sql?.replace(/\s+/g, " ").trim() }));

test("running the exported schema SQL builds the same schema as syncing each table", async () => {
  const synced = new BunDatabase(":memory:");
  const syncedDb = new Database({ adaptor: new BunSqliteAdaptor({ driver: synced }) });
  for (const table of createTables()) await syncedDb.syncTable(table);

  const exported = new BunDatabase(":memory:");
  const exportedDb = new Database({ adaptor: new BunSqliteAdaptor({ driver: exported }) });
  for (const statement of exportedDb.schemaSql(createTables())) exported.run(statement);

  expect(schemaOf(exported)).toEqual(schemaOf(synced));
  expect(
    schemaOf(exported)
      .map((row) => row.name)
      .filter((name) => !name.startsWith("sqlite_")),
  ).toEqual(["asset_project", "asset_project_external_id_unique", "asset", "project"]);
});
