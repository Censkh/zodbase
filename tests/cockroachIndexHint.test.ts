import * as z from "zod";
import { createTable, metaStore, primaryKey, raw, sql } from "../src";
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
});
afterEach(async () => {
  await context?.close();
});

const createItems = () => {
  const items = createTable({
    id: "hinted_items",
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      groupId: z.string(),
      parentId: z.string().nullish(),
      payload: z.string(),
    }),
  });
  // As a project's parent index: no histogram for its second column, only a uniform estimate.
  items.addIndex("hinted_items_parent", [items.$groupId, items.$parentId]);
  return items;
};

/** Runs `run`, returning its result and the plan of the SQL it sent. */
const planOf = async <T>(run: () => Promise<T>) => {
  const { TO_SQL_SYMBOL } = await import("../src/Statement");
  const adaptor = (context.db as any).options.adaptor;
  const execute = adaptor.execute.bind(adaptor);
  let statement = "";
  adaptor.execute = (value: any) => {
    statement = value[TO_SQL_SYMBOL]();
    return execute(value);
  };
  try {
    const result = await run();
    return { result, plan: JSON.stringify((await execute(raw(`EXPLAIN ${statement}`))).results) };
  } finally {
    adaptor.execute = execute;
  }
};

test("looks rows up through a forced index where the planner would scan the table", async () => {
  const items = createItems();
  await context.db.syncTable(items);
  // Statistics without histograms (stale ones, or a column CockroachDB keeps none for): the planner
  // gives each parent the average share of rows, so many parents look like most of the table.
  const payload = "x".repeat(200);
  await context.db.insert(
    items,
    Array.from({ length: 400 }, (_, i) => ({ id: `item-${i}`, groupId: "g", parentId: `old-${i % 4}`, payload })),
  );
  await context.db.execute(sql`ANALYZE ${items}`);
  const [{ statistics }] = (await context.db.execute(sql`SHOW STATISTICS USING JSON FOR TABLE ${items}`)).results as {
    statistics: Record<string, unknown>[];
  }[];
  for (const column of statistics) {
    delete column.histo_buckets;
    delete column.histo_col_type;
  }
  await context.db.execute(
    sql`ALTER TABLE ${items} INJECT STATISTICS ${raw(`'${JSON.stringify(statistics).replaceAll("'", "''")}'`)}`,
  );
  await context.db.insert(items, [
    { id: "new-child-1", groupId: "g", parentId: "new-parent", payload },
    { id: "new-child-2", groupId: "g", parentId: "new-parent", payload },
  ]);
  const parents = ["new-parent", ...Array.from({ length: 40 }, (_, i) => `missing-${i}`)];
  const query = () => context.db.select(items).where(items.$groupId.equals("g").and(items.$parentId.in(parents)));

  const unhinted = await planOf(() => query());
  expect(unhinted.plan).toContain("FULL SCAN");

  const hinted = await planOf(() => query().forceIndex("hinted_items_parent"));
  expect(hinted.plan).toContain("hinted_items@hinted_items_parent");
  expect(hinted.plan).not.toContain("FULL SCAN");
  expect(hinted.result.results.map(({ id }) => id).toSorted()).toEqual(["new-child-1", "new-child-2"]);

  // A condition the index can't constrain fails instead of reading the whole index.
  await expect(
    context.db.select(items).where(items.$payload.equals(payload)).forceIndex("hinted_items_parent").execute(),
  ).rejects.toThrow("NO_FULL_SCAN");
}, 60_000);

test("accepts only the table's own indexes and single-table selects", async () => {
  const items = createItems();
  await context.db.syncTable(items);
  await expect(context.db.select(items).forceIndex("hinted_items_pkey").execute()).rejects.toThrow("Invalid index");
  await expect(
    context.db.select(items).forceIndex(`hinted_items_parent} WHERE true; DROP TABLE hinted_items; --`).execute(),
  ).rejects.toThrow("Invalid index");
  const other = createItems();
  await expect(
    context.db
      .select(items)
      .innerJoin(other, items.$id.equals(other.$parentId))
      .forceIndex("hinted_items_parent")
      .execute(),
  ).rejects.toThrow("An index hint applies to a single-table select");
  expect((await context.db.execute(sql`SELECT count(*) AS n FROM ${items}`)).first).toEqual({ n: "0" });
}, 60_000);
