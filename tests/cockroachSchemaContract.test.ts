import * as zod from "zod";
import { backfill, createTable, metaStore, primaryKey, sql } from "../src";
import {
  acquireCockroachTestContainer,
  releaseCockroachTestContainer,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireCockroachTestContainer, 180_000);
afterAll(releaseCockroachTestContainer, 180_000);

const cockroachFactory = TEST_DATABASE_FACTORIES.find(({ name }) => name === "cockroach")!;
const withBackfill = <TSchema extends zod.ZodType>(schema: TSchema, value: unknown): TSchema =>
  schema.meta(metaStore([backfill({ value })]));

describe("CockroachDB schema synchronization contract", () => {
  let context: TestDatabaseContext;

  beforeEach(async () => {
    context = await cockroachFactory.create();
  });

  afterEach(async () => {
    await context?.close();
  });

  it("discovers named primary constraints and ignores hidden rowid columns", async () => {
    const PrimaryTable = createTable({
      id: "cockroach_primary_people",
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        name: zod.string(),
      }),
    });
    const RowIdTable = createTable({
      id: "cockroach_rowid_people",
      schema: zod.object({ name: zod.string() }),
    });

    await context.db.syncTable(PrimaryTable);
    await context.db.syncTable(PrimaryTable);
    await context.db.syncTable(RowIdTable);
    await context.db.syncTable(RowIdTable);

    await context.db.insert(PrimaryTable, { id: "1", name: "Ada" });
    await context.db.insert(RowIdTable, { name: "Grace" });
    expect((await context.db.select(PrimaryTable, ["*"])).first).toEqual({ id: "1", name: "Ada" });
    expect((await context.db.select(RowIdTable, ["*"])).first).toEqual({ name: "Grace" });

    expect(
      (
        await context.db.execute(sql`
          SHOW CONSTRAINTS FROM "cockroach_primary_people"
        `)
      ).results,
    ).toContainEqual(expect.objectContaining({ constraint_type: "PRIMARY KEY" }));
  });

  it("applies backfilled nullability changes and partial indexes idempotently", async () => {
    const InitialTable = createTable({
      id: "cockroach_schema_people",
      schema: zod.object({ id: zod.string(), email: zod.string().nullable(), obsolete: zod.string() }),
    });
    await context.db.syncTable(InitialTable);
    await context.db.insert(InitialTable, { id: "1", email: null, obsolete: "remove" });

    const UpdatedTable = createTable({
      id: "cockroach_schema_people",
      schema: zod.object({
        id: zod.string(),
        email: withBackfill(zod.string(), "unknown@example.com"),
        role: withBackfill(zod.string(), "user"),
      }),
    });
    UpdatedTable.addIndex("cockroach_email_partial", [UpdatedTable.$email], {
      unique: true,
      where: UpdatedTable.$email.notEquals(null),
    });

    await context.db.syncTable(UpdatedTable);
    await context.db.syncTable(UpdatedTable);

    expect((await context.db.select(UpdatedTable, ["*"])).first).toEqual({
      id: "1",
      email: "unknown@example.com",
      role: "user",
    });
    expect(
      (
        await context.db.execute(sql`
          SHOW INDEX FROM "cockroach_schema_people"
        `)
      ).results.filter((row) => row.index_name === "cockroach_email_partial" && !row.storing),
    ).toHaveLength(1);
  });
});
