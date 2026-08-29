import BunDatabase from "bun:sqlite";
import * as zod from "zod/v4";
import { backfill, createTable, Database, metaStore, primaryKey } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

const withMetadata = <TSchema extends zod.ZodType>(schema: TSchema, ...items: any[]): TSchema =>
  schema.meta(metaStore(items));

describe("schema synchronization contract", () => {
  let driver: BunDatabase;
  let db: Database;

  beforeEach(() => {
    driver = new BunDatabase(":memory:");
    db = new Database({ adaptor: new BunSqliteAdaptor({ driver }) });
  });

  afterEach(() => {
    driver.close();
  });

  it("maps Zod scalar and JSON types to database columns", async () => {
    const TypesTable = createTable({
      id: "schema_types",
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        integerValue: zod.int(),
        realValue: zod.number(),
        booleanValue: zod.boolean(),
        dateValue: zod.date(),
        bigintValue: zod.bigint(),
        objectValue: zod.object({ nested: zod.string() }),
        arrayValue: zod.array(zod.string()),
        optionalValue: zod.string().optional(),
        nullableValue: zod.string().nullable(),
      }),
    });

    await db.syncTable(TypesTable);

    const columns = driver.query("PRAGMA table_info(schema_types)").all() as Array<{
      name: string;
      type: string;
      notnull: number;
      pk: number;
    }>;
    expect(columns.map(({ name, type, notnull, pk }) => ({ name, type, notnull, pk }))).toEqual([
      { name: "id", type: "TEXT", notnull: 1, pk: 1 },
      { name: "integerValue", type: "INTEGER", notnull: 1, pk: 0 },
      { name: "realValue", type: "REAL", notnull: 1, pk: 0 },
      { name: "booleanValue", type: "BOOLEAN", notnull: 1, pk: 0 },
      { name: "dateValue", type: "TIMESTAMP", notnull: 1, pk: 0 },
      { name: "bigintValue", type: "BIGINT", notnull: 1, pk: 0 },
      { name: "objectValue", type: "JSONB", notnull: 1, pk: 0 },
      { name: "arrayValue", type: "JSONB", notnull: 1, pk: 0 },
      { name: "optionalValue", type: "TEXT", notnull: 0, pk: 0 },
      { name: "nullableValue", type: "TEXT", notnull: 0, pk: 0 },
    ]);
  });

  it("adds nullable columns without losing rows", async () => {
    const InitialTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string() }),
    });
    await db.syncTable(InitialTable);
    await db.insert(InitialTable, { id: "1", name: "Ada" });

    const UpdatedTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string(), nickname: zod.string().nullable() }),
    });
    await db.syncTable(UpdatedTable);

    expect((await db.select(UpdatedTable, ["*"])).results).toEqual([{ id: "1", name: "Ada", nickname: null }]);
  });

  it("adds required columns with backfill in one synchronization", async () => {
    const InitialTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string() }),
    });
    await db.syncTable(InitialTable);
    await db.insert(InitialTable, { id: "1", name: "Ada" });

    const UpdatedTable = createTable({
      id: "schema_people",
      schema: zod.object({
        id: zod.string(),
        name: zod.string(),
        role: withMetadata(zod.string(), backfill({ value: "user" })),
      }),
    });
    await db.syncTable(UpdatedTable);

    expect((await db.select(UpdatedTable, ["*"])).results).toEqual([{ id: "1", name: "Ada", role: "user" }]);
    expect(driver.query("PRAGMA table_info(schema_people)").all()).toContainEqual(
      expect.objectContaining({ name: "role", notnull: 1 }),
    );
  });

  it("rejects required additions without backfill before altering the table", async () => {
    const InitialTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string() }),
    });
    await db.syncTable(InitialTable);
    await db.insert(InitialTable, { id: "1", name: "Ada" });

    const InvalidTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string(), role: zod.string() }),
    });

    expect(db.syncTable(InvalidTable)).rejects.toThrow("Backfill value is required");
    expect(driver.query("PRAGMA table_info(schema_people)").all()).not.toContainEqual(
      expect.objectContaining({ name: "role" }),
    );
    expect(driver.query("SELECT * FROM schema_people").all()).toEqual([{ id: "1", name: "Ada" }]);
  });

  it("removes columns while preserving remaining data", async () => {
    const InitialTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string(), obsolete: zod.string() }),
    });
    await db.syncTable(InitialTable);
    await db.insert(InitialTable, { id: "1", name: "Ada", obsolete: "remove me" });

    const UpdatedTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string() }),
    });
    await db.syncTable(UpdatedTable);

    expect((await db.select(UpdatedTable, ["*"])).results).toEqual([{ id: "1", name: "Ada" }]);
    expect(driver.query("PRAGMA table_info(schema_people)").all()).not.toContainEqual(
      expect.objectContaining({ name: "obsolete" }),
    );
  });

  it("adds and removes NOT NULL while preserving data and indexes", async () => {
    const InitialTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), email: zod.string().nullable() }),
    });
    InitialTable.addIndex("schema_people_email_index", [InitialTable.$email]);
    await db.syncTable(InitialTable);
    await db.insertMany(InitialTable, [
      { id: "1", email: null },
      { id: "2", email: "grace@example.com" },
    ]);

    const RequiredTable = createTable({
      id: "schema_people",
      schema: zod.object({
        id: zod.string(),
        email: withMetadata(zod.string(), backfill({ value: "unknown@example.com" })),
      }),
    });
    RequiredTable.addIndex("schema_people_email_index", [RequiredTable.$email]);
    await db.syncTable(RequiredTable);

    expect((await db.select(RequiredTable, ["*"]).orderBy(RequiredTable.$id, "ASC")).results).toEqual([
      { id: "1", email: "unknown@example.com" },
      { id: "2", email: "grace@example.com" },
    ]);
    expect(driver.query("PRAGMA index_list(schema_people)").all()).toContainEqual(
      expect.objectContaining({ name: "schema_people_email_index" }),
    );

    const NullableAgainTable = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), email: zod.string().nullable() }),
    });
    NullableAgainTable.addIndex("schema_people_email_index", [NullableAgainTable.$email]);
    await db.syncTable(NullableAgainTable);

    expect(driver.query("PRAGMA table_info(schema_people)").all()).toContainEqual(
      expect.objectContaining({ name: "email", notnull: 0 }),
    );
    expect((await db.select(NullableAgainTable, ["*"])).results).toHaveLength(2);
  });

  it("is idempotent after a complex synchronization", async () => {
    const Table = createTable({
      id: "schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string(), nickname: zod.string().nullable() }),
    });
    Table.addIndex("schema_people_name_index", [Table.$name]);

    await db.syncTable(Table);
    await db.syncTable(Table);
    await db.syncTable(Table);

    expect(driver.query("PRAGMA index_list(schema_people)").all()).toContainEqual(
      expect.objectContaining({ name: "schema_people_name_index" }),
    );
    expect(driver.query("PRAGMA table_info(schema_people)").all()).toHaveLength(3);
  });
});
