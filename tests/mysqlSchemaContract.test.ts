import * as zod from "zod/v4";
import { backfill, createTable, metaStore, primaryKey, sql } from "../src";
import {
  acquireMysqlTestContainers,
  releaseMysqlTestContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireMysqlTestContainers, 180_000);
afterAll(releaseMysqlTestContainers, 180_000);

const mysqlFactories = TEST_DATABASE_FACTORIES.filter(({ name }) => name === "mysql" || name === "mariadb");
const withBackfill = <TSchema extends zod.ZodType>(schema: TSchema, value: unknown): TSchema =>
  schema.meta(metaStore([backfill({ value })]));

describe.each(mysqlFactories)("$name schema synchronization contract", ({ create, name }) => {
  let context: TestDatabaseContext;

  beforeEach(async () => {
    context = await create();
  });

  afterEach(async () => {
    await context.close();
  });

  const columnInfo = async (tableName: string) =>
    (
      await context.db.execute(sql`
        SELECT
          column_name AS column_name,
          data_type AS data_type,
          is_nullable AS is_nullable
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = ${tableName}
        ORDER BY ordinal_position
      `)
    ).results;

  it("creates and introspects MySQL-family types", async () => {
    const Table = createTable({
      id: "mysql_schema_types",
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        integerValue: zod.int(),
        realValue: zod.number(),
        booleanValue: zod.boolean(),
        timestampValue: zod.date(),
        bigintValue: zod.bigint(),
        jsonValue: zod.object({ value: zod.string() }),
        optionalValue: zod.string().optional(),
      }),
    });

    await context.db.syncTable(Table);

    expect(await columnInfo("mysql_schema_types")).toEqual([
      { column_name: "id", data_type: "varchar", is_nullable: "NO" },
      { column_name: "integerValue", data_type: "int", is_nullable: "NO" },
      { column_name: "realValue", data_type: "double", is_nullable: "NO" },
      { column_name: "booleanValue", data_type: "tinyint", is_nullable: "NO" },
      { column_name: "timestampValue", data_type: "datetime", is_nullable: "NO" },
      { column_name: "bigintValue", data_type: "bigint", is_nullable: "NO" },
      { column_name: "jsonValue", data_type: name === "mysql" ? "json" : "longtext", is_nullable: "NO" },
      { column_name: "optionalValue", data_type: "varchar", is_nullable: "YES" },
    ]);
  });

  it("adds, backfills, constrains, removes, and indexes idempotently", async () => {
    const InitialTable = createTable({
      id: "mysql_schema_people",
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        email: zod.string().nullable(),
        obsolete: zod.string(),
      }),
    });
    await context.db.syncTable(InitialTable);
    await context.db.insert(InitialTable, { id: "1", email: null, obsolete: "remove" });

    const UpdatedTable = createTable({
      id: "mysql_schema_people",
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        email: withBackfill(zod.string(), "unknown@example.com"),
        nickname: zod.string().nullable(),
        role: withBackfill(zod.string(), "user"),
      }),
    });
    UpdatedTable.addIndex("mysql_schema_people_email_index", [UpdatedTable.$email], { unique: true });

    await context.db.syncTable(UpdatedTable);
    await context.db.syncTable(UpdatedTable);

    expect((await context.db.select(UpdatedTable, ["*"])).first).toEqual({
      id: "1",
      email: "unknown@example.com",
      nickname: null,
      role: "user",
    });
    expect(await columnInfo("mysql_schema_people")).not.toContainEqual(
      expect.objectContaining({ column_name: "obsolete" }),
    );
    expect(await columnInfo("mysql_schema_people")).toContainEqual(
      expect.objectContaining({ column_name: "email", is_nullable: "NO" }),
    );
    expect(
      (
        await context.db.execute(sql`
          SELECT COUNT(*) AS count
          FROM information_schema.statistics
          WHERE table_schema = DATABASE()
            AND table_name = ${"mysql_schema_people"}
            AND index_name = ${"mysql_schema_people_email_index"}
        `)
      ).first,
    ).toEqual({ count: 1 });
  });

  it("rejects required additions without backfill before altering populated tables", async () => {
    const InitialTable = createTable({
      id: "mysql_schema_safety",
      schema: zod.object({ id: zod.string() }),
    });
    await context.db.syncTable(InitialTable);
    await context.db.insert(InitialTable, { id: "1" });

    const UnsafeTable = createTable({
      id: "mysql_schema_safety",
      schema: zod.object({ id: zod.string(), requiredValue: zod.string() }),
    });

    await expect(context.db.syncTable(UnsafeTable)).rejects.toThrow("Backfill value is required");
    expect(await columnInfo("mysql_schema_safety")).not.toContainEqual(
      expect.objectContaining({ column_name: "requiredValue" }),
    );
  });

  it("rejects unsupported partial indexes explicitly", async () => {
    const Table = createTable({
      id: "mysql_partial_index",
      schema: zod.object({ id: zod.string(), email: zod.string().nullable() }),
    });
    Table.addIndex("mysql_partial_email", [Table.$email], { where: Table.$email.notEquals(null) });

    await expect(context.db.syncTable(Table)).rejects.toThrow("do not support partial indexes");
  });
});
