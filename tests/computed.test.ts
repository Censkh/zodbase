import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import * as zod from "zod";
import { computed, createTable, metaStore, primaryKey } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const SHELVES: Record<string, string> = { a: "attic", c: "cellar" };

/** A row's shelf, from the first character after its ID's prefix: `item_a…` is on the attic shelf. */
const shelf = computed({
  sql: (column) =>
    `CASE SUBSTR(${column("id")}, 6, 1) ${Object.entries(SHELVES)
      .map(([code, name]) => `WHEN '${code}' THEN '${name}'`)
      .join(" ")} END`,
  value: (row) => SHELVES[String(row.id).charAt(5)],
});

const shelvedSchema = zod.object({
  id: zod.string().meta(metaStore([primaryKey()])),
  name: zod.string(),
  shelf: zod.string().meta(metaStore([shelf])),
});

describe.each(TEST_DATABASE_FACTORIES)("computed columns: $name", ({ create, name }) => {
  let context: TestDatabaseContext;
  let tableId: string;
  beforeEach(async () => {
    context = await create();
    tableId = `shelved_${crypto.randomUUID().slice(0, 8)}`;
  });
  afterEach(async () => context?.close());

  const shelvedTable = () => createTable({ id: tableId, schema: shelvedSchema });

  it("computes the column from the row, whatever a write says for it", async () => {
    const table = shelvedTable();
    await context.db.syncTable(table);
    // The shelf a write gives is ignored: the database computes it.
    const one = context.db.insert(table, { id: "item_a1", name: "lamp", shelf: "cellar" });
    expect(one.rows[0]!.shelf).toBe("attic");
    await one;
    await context.db.insert(table, [
      { id: "item_c1", name: "wine", shelf: "attic" },
      { id: "item_a2", name: "trunk", shelf: "cellar" },
    ]);
    await context.db.update(table, { name: "old lamp", shelf: "cellar" }).where(table.$id.equals("item_a1"));
    const { results } = await context.db.select(table).orderBy(table.$id, "ASC");
    expect(results.map(({ id, name, shelf }) => ({ id, name, shelf }))).toEqual([
      { id: "item_a1", name: "old lamp", shelf: "attic" },
      { id: "item_a2", name: "trunk", shelf: "attic" },
      { id: "item_c1", name: "wine", shelf: "cellar" },
    ]);
  });

  it("returns the computed value with the rows an insert or upsert wrote", async () => {
    const table = shelvedTable();
    await context.db.syncTable(table);
    const inserted = await context.db.insert(table, { id: "item_c2", name: "cheese", shelf: "x" }).returning();
    expect(inserted.first).toMatchObject({ id: "item_c2", shelf: "cellar" });
    const upserted = await context.db
      .insert(table, { id: "item_c2", name: "aged cheese", shelf: "x" })
      .onConflict(table.$id)
      .doUpdate()
      .returning();
    expect(upserted.first).toMatchObject({ id: "item_c2", name: "aged cheese", shelf: "cellar" });
  });

  it("finds nothing to change when a table with a computed column syncs again", async () => {
    const table = shelvedTable();
    await context.db.syncTable(table);
    await context.db.insert(table, { id: "item_a3", name: "chair", shelf: "" });
    await context.db.syncTable(table);
    expect((await context.db.select(table)).results).toEqual([{ id: "item_a3", name: "chair", shelf: "attic" }]);
  });

  it("refuses a column that is plain in the database but computed in its definition", async () => {
    const plain = createTable({
      id: tableId,
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        name: zod.string(),
        shelf: zod.string(),
      }),
    });
    await context.db.syncTable(plain);
    await expect(context.db.syncTable(shelvedTable())).rejects.toThrow(/'shelf'.*migrate it first/);
  });

  if (name === "mysql" || name === "mariadb") {
    it("refuses to add a computed column to an existing table", async () => {
      await context.db.syncTable(createTable({ id: tableId, schema: shelvedSchema.omit({ shelf: true }) }));
      await expect(context.db.syncTable(shelvedTable())).rejects.toThrow(/computed column 'shelf'/);
    });
  } else {
    it("adds a computed column to an existing table, computed for the rows it holds", async () => {
      const before = createTable({ id: tableId, schema: shelvedSchema.omit({ shelf: true }) });
      await context.db.syncTable(before);
      await context.db.insert(before, { id: "item_c3", name: "jam" });
      const table = shelvedTable();
      await context.db.syncTable(table);
      expect((await context.db.select(table)).results).toEqual([{ id: "item_c3", name: "jam", shelf: "cellar" }]);
    });
  }
});
