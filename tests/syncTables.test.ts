import BunDatabase from "bun:sqlite";
import * as z from "zod";
import { createTable, Database, foreignKey, metaStore, primaryKey, raw } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const OwnerTable = createTable({
  id: "sync_owner",
  schema: z.object({ id: z.string().meta(metaStore([primaryKey()])) }),
});
const widgetSchema = {
  id: z.string().meta(metaStore([primaryKey()])),
  ownerId: z.string().meta(metaStore([foreignKey({ field: OwnerTable.$id })])),
  name: z.string(),
};
const WidgetTable = createTable({ id: "sync_widget", schema: z.object(widgetSchema) });
WidgetTable.addIndex("sync_widget_name", [WidgetTable.$name]);
// The same table a release later: a new column and a new unique index.
const NextWidgetTable = createTable({
  id: "sync_widget",
  schema: z.object({ ...widgetSchema, colour: z.string().optional() }),
});
NextWidgetTable.addIndex("sync_widget_name", [NextWidgetTable.$name]);
NextWidgetTable.addIndex("sync_widget_owner_colour", [NextWidgetTable.$ownerId, NextWidgetTable.$colour], {
  unique: true,
});
const GadgetTable = createTable({
  id: "sync_gadget",
  schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), label: z.string() }),
});
GadgetTable.addIndex("sync_gadget_label", [GadgetTable.$label]);

// MySQL-compatible adaptors don't introspect schemas.
const factories = TEST_DATABASE_FACTORIES.filter(({ name }) => !["mysql", "mariadb"].includes(name));

describe.each(factories)("syncTables: $name", ({ create }) => {
  let context: TestDatabaseContext;
  beforeEach(async () => {
    context = await create();
  });
  afterEach(async () => {
    await context.close();
  });

  it("brings a schema missing a table, a column and indexes up to the definitions", async () => {
    await context.db.syncTables([OwnerTable, WidgetTable]);
    await context.db.syncTables([OwnerTable, NextWidgetTable, GadgetTable]);

    const schema = await context.db.describeSchema();
    const table = (name: string) => schema.find((live) => live.name === name);
    expect(table("sync_owner")?.columns).toEqual(["id"]);
    expect(table("sync_widget")?.columns.toSorted()).toEqual(["colour", "id", "name", "ownerId"]);
    expect(table("sync_widget")?.indexes.toSorted((a, b) => a.name.localeCompare(b.name))).toEqual([
      { name: "sync_widget_name", unique: false },
      { name: "sync_widget_owner_colour", unique: true },
    ]);
    expect(table("sync_gadget")?.columns.toSorted()).toEqual(["id", "label"]);
    expect(table("sync_gadget")?.indexes).toEqual([{ name: "sync_gadget_label", unique: false }]);
    expect(await context.db.diffSchema([OwnerTable, NextWidgetTable, GadgetTable])).toEqual({
      tables: [],
      columns: [],
      indexes: [],
    });
  });

  it("restores an index dropped from an otherwise current table", async () => {
    await context.db.syncTables([OwnerTable, WidgetTable]);
    await context.db.execute(raw('DROP INDEX "sync_widget_name"'));

    await context.db.syncTables([OwnerTable, WidgetTable]);

    const widget = (await context.db.describeSchema()).find((live) => live.name === "sync_widget");
    expect(widget?.indexes).toEqual([{ name: "sync_widget_name", unique: false }]);
  });
});

describe("syncTables on SQLite", () => {
  /** The statements syncing these tables runs once they are already up to date. */
  const resyncStatements = async (tables: Parameters<Database["syncTables"]>[0]) => {
    const driver = new BunDatabase(":memory:");
    const statements: string[] = [];
    const prepare = driver.prepare.bind(driver);
    driver.prepare = ((query: string) => {
      statements.push(query.trim());
      return prepare(query);
    }) as typeof driver.prepare;
    const db = new Database({ adaptor: new BunSqliteAdaptor({ driver }) });
    await db.syncTables(tables);
    statements.length = 0;
    await db.syncTables(tables);
    driver.close();
    return statements;
  };

  it("only reads an up-to-date schema, changing nothing", async () => {
    const statements = await resyncStatements([OwnerTable, NextWidgetTable, GadgetTable]);

    const reads = /^(SELECT\b|PRAGMA (table_xinfo|foreign_key_list|index_list)\()/i;
    expect(statements.filter((statement) => !reads.test(statement))).toEqual([]);
  });
});
