import * as zod from "zod/v4";
import { backfill, createTable, metaStore, sql } from "../src";
import {
  acquirePostgresTestContainer,
  releasePostgresTestContainer,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquirePostgresTestContainer, 120_000);
afterAll(releasePostgresTestContainer, 120_000);

const postgresFactory = TEST_DATABASE_FACTORIES.find(({ name }) => name === "postgres")!;
const withBackfill = <TSchema extends zod.ZodType>(schema: TSchema, value: unknown): TSchema =>
  schema.meta(metaStore([backfill({ value })]));

describe("PostgreSQL schema synchronization contract", () => {
  let context: TestDatabaseContext;

  beforeEach(async () => {
    context = await postgresFactory.create();
  });

  afterEach(async () => {
    await context.close();
  });

  const columnInfo = async (tableName: string) =>
    (
      await context.db.execute(sql`
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = ${tableName}
        ORDER BY ordinal_position
      `)
    ).results;

  it("creates and introspects PostgreSQL types", async () => {
    const Table = createTable({
      id: "pg_schema_types",
      schema: zod.object({
        textValue: zod.string(),
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

    expect(await columnInfo("pg_schema_types")).toEqual([
      { column_name: "textValue", data_type: "text", is_nullable: "NO" },
      { column_name: "integerValue", data_type: "integer", is_nullable: "NO" },
      { column_name: "realValue", data_type: "real", is_nullable: "NO" },
      { column_name: "booleanValue", data_type: "boolean", is_nullable: "NO" },
      { column_name: "timestampValue", data_type: "timestamp without time zone", is_nullable: "NO" },
      { column_name: "bigintValue", data_type: "bigint", is_nullable: "NO" },
      { column_name: "jsonValue", data_type: "jsonb", is_nullable: "NO" },
      { column_name: "optionalValue", data_type: "text", is_nullable: "YES" },
    ]);
  });

  it("adds nullable and required backfilled columns without losing data", async () => {
    const InitialTable = createTable({
      id: "pg_schema_people",
      schema: zod.object({ id: zod.string(), name: zod.string() }),
    });
    await context.db.syncTable(InitialTable);
    await context.db.insert(InitialTable, { id: "1", name: "Ada" });

    const UpdatedTable = createTable({
      id: "pg_schema_people",
      schema: zod.object({
        id: zod.string(),
        name: zod.string(),
        nickname: zod.string().nullable(),
        role: withBackfill(zod.string(), "user"),
      }),
    });
    await context.db.syncTable(UpdatedTable);

    expect((await context.db.select(UpdatedTable, ["*"])).first).toEqual({
      id: "1",
      name: "Ada",
      nickname: null,
      role: "user",
    });
    expect(await columnInfo("pg_schema_people")).toContainEqual(
      expect.objectContaining({ column_name: "role", is_nullable: "NO" }),
    );
  });

  it("changes nullability with backfill", async () => {
    const InitialTable = createTable({
      id: "pg_schema_people",
      schema: zod.object({ id: zod.string(), email: zod.string().nullable() }),
    });
    await context.db.syncTable(InitialTable);
    await context.db.insertMany(InitialTable, [
      { id: "1", email: null },
      { id: "2", email: "grace@example.com" },
    ]);

    const RequiredTable = createTable({
      id: "pg_schema_people",
      schema: zod.object({ id: zod.string(), email: withBackfill(zod.string(), "unknown@example.com") }),
    });
    await context.db.syncTable(RequiredTable);

    expect(await columnInfo("pg_schema_people")).toContainEqual(
      expect.objectContaining({ column_name: "email", is_nullable: "NO" }),
    );
    expect((await context.db.select(RequiredTable, ["*"]).orderBy(RequiredTable.$id, "ASC")).results).toEqual([
      { id: "1", email: "unknown@example.com" },
      { id: "2", email: "grace@example.com" },
    ]);
  });

  it("removes columns and keeps declared indexes idempotently", async () => {
    const InitialTable = createTable({
      id: "pg_schema_people",
      schema: zod.object({ id: zod.string(), email: zod.string(), obsolete: zod.string() }),
    });
    await context.db.syncTable(InitialTable);
    await context.db.insert(InitialTable, { id: "1", email: "ada@example.com", obsolete: "remove" });

    const UpdatedTable = createTable({
      id: "pg_schema_people",
      schema: zod.object({ id: zod.string(), email: zod.string() }),
    });
    UpdatedTable.addIndex("pg_schema_people_email_index", [UpdatedTable.$email], { unique: true });
    await context.db.syncTable(UpdatedTable);
    await context.db.syncTable(UpdatedTable);

    expect((await context.db.select(UpdatedTable, ["*"])).first).toEqual({ id: "1", email: "ada@example.com" });
    expect(await columnInfo("pg_schema_people")).not.toContainEqual(
      expect.objectContaining({ column_name: "obsolete" }),
    );
    expect(
      (
        await context.db.execute(sql`
          SELECT COUNT(*)::int AS count
          FROM pg_indexes
          WHERE indexname = ${"pg_schema_people_email_index"}
        `)
      ).first,
    ).toEqual({ count: 1 });
  });
});
