import * as z from "zod";
import { cockroachLocality, cockroachRegion } from "zodbase/adaptors/cockroach";
import { createTable, foreignKey, metaStore, primaryKey, raw, sql } from "../src";
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
  expect((await context.db.select(children, ["*"])).first).toEqual({
    id: "c",
    userId: "u",
    homeRegion: "aws-ap-southeast-1",
  });
  expect((await context.db.execute(sql`SHOW CREATE TABLE regional_children`)).first?.create_statement).toContain(
    'LOCALITY REGIONAL BY ROW AS "homeRegion"',
  );
  await context.db.delete(users).where(users.$id.equals("u"));
  expect((await context.db.select(children, ["*"])).results).toHaveLength(0);
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
  expect((await context.db.select(table, ["*"])).first).toEqual({ id: "one" });
}, 60_000);
