import * as z from "zod";
import { cockroachLocality, cockroachRegion } from "zodbase/adaptors/cockroach";
import { computed, createTable, foreignKey, metaStore, primaryKey, raw, sql } from "../src";
import {
  acquireCockroachTestContainer,
  releaseCockroachTestContainer,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireCockroachTestContainer, 180_000);
afterAll(releaseCockroachTestContainer, 180_000);
const factory = TEST_DATABASE_FACTORIES.find(({ name }) => name === "cockroach")!;
let context: TestDatabaseContext;
beforeEach(async () => {
  context = await factory.create();
  const { first } = await context.db.execute(sql`SELECT current_database() AS name`);
  await context.db.execute(sql`ALTER DATABASE ${raw(`"${first!.name}"`)} PRIMARY REGION "aws-ap-southeast-1"`);
});
afterEach(async () => {
  await context?.close();
});

test("preserves regional rows, foreign keys and indexes across repeated syncs", async () => {
  const region = z.string().meta(metaStore([cockroachRegion()]));
  const users = createTable({
    id: "regional_users",
    schema: z
      .object({
        id: z.string().meta(metaStore([primaryKey()])),
        homeRegion: region,
      })
      .meta(metaStore([cockroachLocality({ type: "regional-by-row", regionColumn: "homeRegion" })])),
  });
  const children = createTable({
    id: "regional_children",
    schema: z
      .object({
        id: z.string().meta(metaStore([primaryKey()])),
        homeRegion: region,
        userId: z.string().meta(metaStore([foreignKey({ field: users.$id, onDelete: "cascade" })])),
      })
      .meta(metaStore([cockroachLocality({ type: "regional-by-row", regionColumn: "homeRegion" })])),
  });
  children.addIndex("regional_children_user", [children.$userId]);
  await context.db.syncTable(users);
  await context.db.syncTable(children);
  await context.db.insert(users, { id: "u", homeRegion: "aws-ap-southeast-1" });
  await context.db.insert(children, { id: "c", userId: "u", homeRegion: "aws-ap-southeast-1" });
  await context.db.syncTable(users);
  await context.db.syncTable(children);
  expect((await context.db.select(children)).first).toEqual({
    id: "c",
    userId: "u",
    homeRegion: "aws-ap-southeast-1",
  });
  expect((await context.db.execute(sql`SHOW CREATE TABLE regional_children`)).first?.create_statement).toContain(
    'LOCALITY REGIONAL BY ROW AS "homeRegion"',
  );
  await context.db.delete(users).where(users.$id.equals("u"));
  expect((await context.db.select(children)).results).toHaveLength(0);
}, 60_000);

test("preserves Cockroach's implicit region column when syncing", async () => {
  const table = createTable({
    id: "implicit_region",
    schema: z
      .object({ id: z.string().meta(metaStore([primaryKey()])) })
      .meta(metaStore([cockroachLocality({ type: "regional-by-row" })])),
  });
  await context.db.syncTable(table);
  await context.db.insert(table, { id: "one" });
  await context.db.syncTable(table);
  expect((await context.db.select(table)).first).toEqual({ id: "one" });
}, 60_000);

test("places each row in the region its computed region column names, and resyncs unchanged", async () => {
  const table = createTable({
    id: "computed_region",
    schema: z
      .object({
        id: z.string().meta(metaStore([primaryKey()])),
        name: z.string(),
        // The region is the ID's: `a…` IDs live in aws-ap-southeast-1.
        homeRegion: z.string().meta(
          metaStore([
            cockroachRegion(),
            computed({
              sql: (column) => `CASE substr(${column("id")}, 1, 1) WHEN 'a' THEN 'aws-ap-southeast-1' END`,
              value: (row) => (String(row.id).startsWith("a") ? "aws-ap-southeast-1" : undefined),
            }),
          ]),
        ),
      })
      .meta(metaStore([cockroachLocality({ type: "regional-by-row", regionColumn: "homeRegion" })])),
  });
  await context.db.syncTable(table);
  const inserted = await context.db.insert(table, { id: "a1", name: "first", homeRegion: "ignored" }).returning();
  expect(inserted.first).toEqual({ id: "a1", name: "first", homeRegion: "aws-ap-southeast-1" });
  // An ID naming no region has none, which the column refuses.
  await expect(
    Promise.resolve(context.db.insert(table, { id: "z1", name: "nowhere", homeRegion: "" })),
  ).rejects.toThrow(/null/i);
  await context.db.syncTable(table);
  // As the database stored it, not as the insert computed it: the column partitions the row.
  const { first } = await context.db.execute(
    sql`SELECT "homeRegion"::STRING AS region FROM computed_region WHERE id = 'a1'`,
  );
  expect(first?.region).toBe("aws-ap-southeast-1");
  expect((await context.db.execute(sql`SHOW CREATE TABLE computed_region`)).first?.create_statement).toContain(
    'LOCALITY REGIONAL BY ROW AS "homeRegion"',
  );
}, 60_000);

test("regional scalar inserts retain point lookups and the auto-commit fast path", async () => {
  const region = z.string().meta(metaStore([cockroachRegion()]));
  const locality = metaStore([cockroachLocality({ type: "regional-by-row", regionColumn: "homeRegion" })]);
  const parent = createTable({
    id: "plan_regional_parent",
    schema: z
      .object({
        id: z.string().meta(metaStore([primaryKey()])),
        homeRegion: region,
      })
      .meta(locality),
  });
  const child = createTable({
    id: "plan_regional_child",
    schema: z
      .object({
        id: z.string().meta(metaStore([primaryKey()])),
        homeRegion: region,
      })
      .meta(locality),
  });
  const { db } = context;
  await db.syncTable(parent);
  await db.syncTable(child);
  await db.insert(
    parent,
    Array.from({ length: 128 }, (_, i) => ({ id: `parent-${i}`, homeRegion: "aws-ap-southeast-1" })),
  );
  await db.execute(sql`ANALYZE ${parent}`);
  const { TO_SQL_SYMBOL } = await import("../src/Statement");
  const adaptor = (db as any).options.adaptor;
  const execute = adaptor.execute.bind(adaptor);
  let statement = "";
  adaptor.execute = (value: any) => {
    statement = value[TO_SQL_SYMBOL]();
    return execute(value);
  };
  const homeRegion = db.select(parent, { homeRegion: parent.$homeRegion }).where(parent.$id.equals("parent-64"));
  await db.insert(child, [
    { id: "one", homeRegion },
    { id: "two", homeRegion },
  ]);
  const plan = JSON.stringify((await execute(raw(`EXPLAIN ${statement}`))).results);
  expect(plan).toContain("plan_regional_parent_pkey");
  expect(plan).not.toContain("FULL SCAN");
  expect(plan).toContain("auto commit");
  expect(statement).not.toContain("CAST");
  expect((await db.select(child, { homeRegion: child.$homeRegion })).results).toEqual([
    { homeRegion: "aws-ap-southeast-1" },
    { homeRegion: "aws-ap-southeast-1" },
  ]);
}, 60_000);

test("converts a regional-by-row parent to global while keeping its region column and children", async () => {
  const region = z.string().meta(metaStore([cockroachRegion()]));
  const regionalByRow = metaStore([cockroachLocality({ type: "regional-by-row", regionColumn: "homeRegion" })]);
  const parentSchema = z.object({
    id: z.string().meta(metaStore([primaryKey()])),
    homeRegion: region,
    userId: z.string(),
  });
  const regionalParent = createTable({ id: "global_parent", schema: parentSchema.meta(regionalByRow) });
  regionalParent.addIndex("global_parent_user", [regionalParent.$userId]);
  const globalParent = createTable({
    id: "global_parent",
    schema: parentSchema.meta(metaStore([cockroachLocality({ type: "global" })])),
  });
  globalParent.addIndex("global_parent_user", [globalParent.$userId]);
  const child = createTable({
    id: "global_parent_child",
    schema: z
      .object({
        id: z.string().meta(metaStore([primaryKey()])),
        homeRegion: region,
        parentId: z.string().meta(metaStore([foreignKey({ field: regionalParent.$id, onDelete: "cascade" })])),
      })
      .meta(regionalByRow),
  });
  const { db } = context;
  await db.syncTable(regionalParent);
  await db.syncTable(child);
  await db.insert(regionalParent, { id: "p", homeRegion: "aws-ap-southeast-1", userId: "u" });
  await db.insert(child, { id: "c", parentId: "p", homeRegion: "aws-ap-southeast-1" });

  await db.syncTable(globalParent);
  await db.syncTable(child);
  await db.syncTable(globalParent);

  const createStatement = async (table: string) =>
    String((await db.execute(sql`SHOW CREATE TABLE ${raw(table)}`)).first?.create_statement);
  expect(await createStatement("global_parent")).toContain("LOCALITY GLOBAL");
  expect(await createStatement("global_parent_child")).toContain('LOCALITY REGIONAL BY ROW AS "homeRegion"');
  expect((await db.select(globalParent).where(globalParent.$userId.equals("u"))).first).toEqual({
    id: "p",
    homeRegion: "aws-ap-southeast-1",
    userId: "u",
  });
  await db.delete(globalParent).where(globalParent.$id.equals("p"));
  expect((await db.select(child)).results).toHaveLength(0);
}, 120_000);
