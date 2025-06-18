import BunDatabase from "bun:sqlite";
import { newDb } from "pg-mem";
import * as zod from "zod/v4";
import { and, createTable, Database, meta, or, primaryKey, sql, TO_SQL_SYMBOL } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";
import PostgresAdaptor from "../src/adaptors/postgres";

const evilStrings = [
  "' OR 1=1; --",
  "Robert'); DROP TABLE test;--",
  "'; SELECT pg_sleep(10); --",
  "'; insert into test (id, name) values (1, 'hacker'); --",
];

const TestTable = createTable({
  id: "test",
  schema: zod.object({
    id: zod.string().meta(meta([primaryKey()])),
    data: zod.any(),
  }),
});

const createBunDb = async () => {
  const rawDb = new BunDatabase(":memory:");
  const db = new Database({
    adaptor: new BunSqliteAdaptor({
      driver: rawDb,
    }),
  });
  await db.syncTable(TestTable);
  return db;
};

const createPgMockDb = async () => {
  const mem = newDb();
  const { Client } = mem.adapters.createPg();
  const client = new Client();
  await client.connect();

  const db = new Database({
    adaptor: new PostgresAdaptor({
      driver: client,
    }),
  });
  await db.syncTable(TestTable);
  return db;
};

describe.each([
  { name: "pg-mem", createDb: createPgMockDb },
  { name: "bun-sqlite", createDb: createBunDb },
])("SQL Escaping for $name", ({ createDb, name }) => {
  let db: Database;

  beforeEach(async () => {
    db = await createDb();
  });

  it("1. should escape values in insert and select", async () => {
    for (const evilString of evilStrings) {
      const item = { id: "1", data: evilString };
      await db.insert(TestTable, item);

      const { first } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
      expect(first).toEqual(item);

      const { first: firstByData } = await db.select(TestTable, ["*"]).where(TestTable.$data.equals(evilString));
      expect(firstByData).toEqual(item);

      // Clean up for next iteration
      await db.delete(TestTable).where(TestTable.$id.equals("1"));
    }
  });

  it("2. should escape values in count", async () => {
    for (const evilString of evilStrings) {
      const item = { id: "1", data: evilString };
      await db.insert(TestTable, item);

      const result = await db.count(TestTable).where(TestTable.$data.equals(evilString));
      expect((result.first as any)?._count).toBe(1);

      // Clean up for next iteration
      await db.delete(TestTable).where(TestTable.$id.equals("1"));
    }
  });

  it("3. should escape values in update", async () => {
    for (const evilString of evilStrings) {
      const item = { id: "1", data: "initial" };
      await db.insert(TestTable, item);

      await db.update(TestTable, { data: evilString }, TestTable.$id.equals("1"));

      const { first } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
      expect(first?.data).toBe(evilString);

      // also test where clause
      await db.update(TestTable, { data: "updated" }, TestTable.$data.equals(evilString));

      const { first: second } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
      expect(second?.data).toBe("updated");

      // Clean up for next iteration
      await db.delete(TestTable).where(TestTable.$id.equals("1"));
    }
  });

  it("4. should escape values in upsert", async () => {
    for (const evilString of evilStrings) {
      if (name !== "pg-mem") {
        // Test insert part of upsert
        const item = { id: "1", data: evilString };
        await db.upsert(TestTable, item, TestTable.$id);
        const { first } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
        expect(first).toEqual(item);

        // Test update part of upsert
        const updatedItem = { id: "1", data: `updated ${evilString}` };
        await db.upsert(TestTable, updatedItem, TestTable.$id);
        const { first: second } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
        expect(second).toEqual(updatedItem);

        // Test where part of upsert (implicitly) by selecting back
        const { first: third } = await db.select(TestTable, ["*"]).where(TestTable.$data.equals(updatedItem.data));
        expect(third).toEqual(updatedItem);

        // Clean up for next iteration
        await db.delete(TestTable).where(TestTable.$id.equals("1"));
      }
    }
  });

  it("5. should escape values in updateMany", async () => {
    const items = [
      { id: "1", data: "a" },
      { id: "2", data: "b" },
    ];
    await db.insertMany(TestTable, items);

    const updatedItems = [
      { id: "1", data: evilStrings[0] },
      { id: "2", data: evilStrings[1] },
    ];
    await db.updateMany(TestTable, updatedItems as any, TestTable.$id as any);

    const { results } = await db.select(TestTable, ["*"]).where(TestTable.$id.in(["1", "2"]));
    // sort to ensure order
    results.sort((a, b) => a.id.localeCompare(b.id));
    updatedItems.sort((a, b) => a.id.localeCompare(b.id));

    expect(results).toEqual(updatedItems);
  });

  it("6. should escape values in delete", async () => {
    for (const evilString of evilStrings) {
      const item1 = { id: "1", data: evilString };
      const item2 = { id: "2", data: "safe" };
      await db.insertMany(TestTable, [item1, item2]);

      await db.delete(TestTable).where(TestTable.$data.equals(evilString));

      const { results } = await db.select(TestTable, ["*"]);
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual(item2);

      // Clean up
      await db.delete(TestTable).where(TestTable.$id.equals("2"));
    }
  });

  it("7. should escape values in insertMany", async () => {
    const items = [
      { id: "1", data: evilStrings[0] },
      { id: "2", data: evilStrings[1] },
    ];
    await db.insertMany(TestTable, items);

    const { results } = await db.select(TestTable, ["*"]);
    expect(results).toHaveLength(2);
    expect(results).toEqual(expect.arrayContaining(items));
  });

  it("8. should escape values with sql template tag", async () => {
    for (const evilString of evilStrings) {
      const id = "1";
      const data = evilString;
      const item = { id, data };
      await db.execute(sql`INSERT INTO test (id, data) VALUES (${id}, ${data})`);

      const { first } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
      expect(first).toEqual(item);

      const result = await db.execute(sql`SELECT * FROM test WHERE data = ${evilString}`);
      expect(result.results[0]).toEqual(item);

      // Clean up
      await db.delete(TestTable).where(TestTable.$id.equals("1"));
    }
  });

  it("9. should handle evil strings in objects", async () => {
    for (let i = 0; i < evilStrings.length; i++) {
      const evilKey = evilStrings[i];
      const otherEvilKey = evilStrings[(i + 1) % evilStrings.length];
      const item = { id: "1", data: { [evilKey]: "value" } };

      // Test insert
      await db.insert(TestTable, item);

      // Test select
      const { first } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
      expect(first).toEqual(item);

      // Test select by data
      const { first: firstByData } = await db.select(TestTable, ["*"]).where(TestTable.$data.equals(item.data));
      expect(firstByData).toEqual(item);

      // Test update
      const newItem = { id: "1", data: { [otherEvilKey]: "new_value" } };
      await db.update(TestTable, { data: newItem.data }, TestTable.$id.equals("1"));
      const { first: updated } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
      expect(updated).toEqual(newItem);

      // Test update where
      await db.update(TestTable, { data: { safe: "value" } }, TestTable.$data.equals(newItem.data));
      const { first: updatedByData } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
      expect(updatedByData?.data).toEqual({ safe: "value" });

      // Clean up for upsert test
      await db.delete(TestTable).where(TestTable.$id.equals("1"));

      if (name !== "pg-mem") {
        // Test upsert
        const upsertItem = { id: "2", data: { [evilKey]: "upserted" } };
        await db.upsert(TestTable, upsertItem, TestTable.$id);
        const { first: upserted } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("2"));
        expect(upserted).toEqual(upsertItem);

        // Test upsert (update part)
        const upsertUpdatedItem = { id: "2", data: { [otherEvilKey]: "upserted_updated" } };
        await db.upsert(TestTable, upsertUpdatedItem, TestTable.$id);
        const { first: upsertedUpdated } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("2"));
        expect(upsertedUpdated).toEqual(upsertUpdatedItem);

        // Clean up
        await db.delete(TestTable).where(TestTable.$id.equals("2"));
      }
    }
  });

  it("10. should strip evil keys at the root of objects", async () => {
    for (const evilKey of evilStrings) {
      const initialItem = { id: "1", data: "initial" };
      await db.insert(TestTable, initialItem);

      // Test that insert strips the evil key
      const insertItem = { id: "2", data: "inserted", [evilKey]: "evil_value" };
      // We use `as any` because TS would otherwise correctly complain that `evilKey` is not on the type
      await db.insert(TestTable, insertItem as any);

      const { first: firstInserted } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("2"));
      expect(firstInserted).toEqual({ id: "2", data: "inserted" });
      expect(firstInserted).not.toHaveProperty(evilKey);

      // Test that update strips the evil key
      const updatePayload = { data: "updated", [evilKey]: "evil_value" };
      await db.update(TestTable, updatePayload as any, TestTable.$id.equals("1"));

      const { first: firstUpdated } = await db.select(TestTable, ["*"]).where(TestTable.$id.equals("1"));
      expect(firstUpdated).toEqual({ id: "1", data: "updated" });
      expect(firstUpdated).not.toHaveProperty(evilKey);

      // Clean up
      await db.delete(TestTable).where(TestTable.$id.in(["1", "2"]));
    }
  });

  it("11. should escape values in select with various conditions", async () => {
    const evilItem = { id: "1", data: evilStrings[0] };
    const safeItem1 = { id: "2", data: "safe1" };
    const safeItem2 = { id: "3", data: "safe2" };
    const evilItem2 = { id: "4", data: evilStrings[1] };

    await db.insertMany(TestTable, [evilItem, safeItem1, safeItem2, evilItem2]);

    // Test notEquals
    const { results: notEqualsResults } = await db
      .select(TestTable, ["*"])
      .where(TestTable.$data.notEquals(evilStrings[0]));

    const expectedNotEquals = [safeItem1, safeItem2, evilItem2];
    expect(notEqualsResults).toHaveLength(expectedNotEquals.length);
    expect(notEqualsResults).toEqual(expect.arrayContaining(expectedNotEquals));

    // Test in
    const { results: inResults } = await db
      .select(TestTable, ["*"])
      .where(TestTable.$data.in([evilStrings[0], "safe1"]));

    const expectedIn = [evilItem, safeItem1];
    expect(inResults).toHaveLength(expectedIn.length);
    expect(inResults).toEqual(expect.arrayContaining(expectedIn));

    // Test or
    const { results: orResults } = await db
      .select(TestTable, ["*"])
      .where(TestTable.$data.equals(evilStrings[0]).or(TestTable.$data.equals(evilStrings[1])));

    const expectedOr = [evilItem, evilItem2];
    expect(orResults).toHaveLength(expectedOr.length);
    expect(orResults).toEqual(expect.arrayContaining(expectedOr));

    // Test and
    const { results: andResults } = await db
      .select(TestTable, ["*"])
      .where(TestTable.$data.equals(evilStrings[0]).and(TestTable.$id.equals("1")));

    expect(andResults).toHaveLength(1);
    expect(andResults[0]).toEqual(evilItem);

    // cleanup
    await db.delete(TestTable).where(TestTable.$id.in(["1", "2", "3", "4"]));
  });
});
