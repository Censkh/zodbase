import * as z from "zod";
import { createTable, foreignKey, metaStore, primaryKey, raw } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const OwnerTable = createTable({
  id: "drift_owner",
  schema: z.object({ id: z.string().meta(metaStore([primaryKey()])) }),
});
const WidgetTable = createTable({
  id: "drift_widget",
  schema: z.object({
    id: z.string().meta(metaStore([primaryKey()])),
    ownerId: z.string().meta(metaStore([foreignKey({ field: OwnerTable.$id })])),
    name: z.string(),
  }),
});
WidgetTable.addIndex("drift_widget_owner_name", [WidgetTable.$ownerId, WidgetTable.$name], { unique: true });
WidgetTable.addIndex("drift_widget_name", [WidgetTable.$name]);
const TABLES = [OwnerTable, WidgetTable];

// MySQL-compatible adaptors don't introspect schemas.
const factories = TEST_DATABASE_FACTORIES.filter(({ name }) => !["mysql", "mariadb"].includes(name));

describe.each(factories)("schema diff: $name", ({ name: engine, create }) => {
  let context: TestDatabaseContext;
  beforeEach(async () => {
    context = await create();
    for (const table of TABLES) await context.db.syncTable(table);
  });
  afterEach(async () => {
    await context.close();
  });

  it("finds nothing the definitions don't define on a freshly synced schema", async () => {
    expect(await context.db.diffSchema(TABLES)).toEqual({ tables: [], columns: [], indexes: [] });
  });

  it("names exactly the tables, columns, indexes and unique constraints the definitions don't, and changes nothing", async () => {
    const sqlite = engine === "bun-sqlite" || engine === "turso-local";
    for (const statement of [
      'CREATE TABLE "drift_stray" ("id" TEXT PRIMARY KEY)',
      "INSERT INTO \"drift_stray\" (\"id\") VALUES ('a'), ('b')",
      'CREATE TABLE "drift_empty" ("id" TEXT PRIMARY KEY)',
      'ALTER TABLE "drift_widget" ADD COLUMN "legacy" TEXT',
      'CREATE INDEX "drift_widget_legacy" ON "drift_widget" ("legacy")',
      // SQLite can't add a constraint to an existing table.
      ...(sqlite ? [] : ['ALTER TABLE "drift_widget" ADD CONSTRAINT "drift_widget_legacy_key" UNIQUE ("legacy")']),
    ])
      await context.db.execute(raw(statement));

    const expected = {
      tables: [
        { name: "drift_empty", rowCount: 0 },
        { name: "drift_stray", rowCount: 2 },
      ],
      columns: [{ table: "drift_widget", column: "legacy" }],
      indexes: [
        { table: "drift_widget", index: "drift_widget_legacy", unique: false },
        ...(sqlite ? [] : [{ table: "drift_widget", index: "drift_widget_legacy_key", unique: true }]),
      ],
    };
    expect(await context.db.diffSchema(TABLES)).toEqual(expected);
    // Read-only: a second look finds the same, and the stray rows are still there.
    expect(await context.db.diffSchema(TABLES)).toEqual(expected);
  });
});
