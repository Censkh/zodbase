import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";
import PostgresAdaptor from "../src/adaptors/postgres";

const TestTable = createTable({
  id: "returning_test",
  schema: zod.object({
    id: zod.string(),
    name: zod.string(),
  }),
});

const createPostgresSqlCaptureDb = () => {
  const queries: string[] = [];
  const driver = {
    async query(sql: string) {
      queries.push(sql);
      return { rows: [] };
    },
  };

  const db = new Database({
    adaptor: new PostgresAdaptor({
      driver: driver as any,
    }),
  });

  return { db, queries };
};

describe("returning", () => {
  it("does not use RETURNING for insert unless mutated rows are requested", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    const result = await db.insert(TestTable, {
      id: "1",
      name: "First",
    });

    expect(queries).toHaveLength(1);
    expect(queries[0]).not.toContain("RETURNING");
    expect(result).toEqual({
      results: [],
      first: undefined,
    });
  });

  it("does not use RETURNING for insertMany unless mutated rows are requested", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    const result = await db.insert(TestTable, [
      {
        id: "1",
        name: "First",
      },
      {
        id: "2",
        name: "Second",
      },
    ]);

    expect(queries).toHaveLength(1);
    expect(queries[0]).not.toContain("RETURNING");
    expect(result).toEqual({
      results: [],
      first: undefined,
    });
  });

  it("uses RETURNING for insert when mutated rows are requested", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    await db
      .insert(TestTable, {
        id: "1",
        name: "First",
      })
      .returning();

    expect(queries).toHaveLength(1);
    expect(queries[0]).toContain("RETURNING *");
  });

  it("gives an insert's parsed rows without RETURNING", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    const insert = db.insert(TestTable, { id: "1", name: "First" });
    // Parsed before anything runs.
    expect(insert.rows).toEqual([{ id: "1", name: "First" }]);
    expect(queries).toHaveLength(0);
    await insert;
    expect(queries).toHaveLength(1);
    expect(queries[0]).not.toContain("RETURNING");
  });

  it("uses RETURNING for insertMany when mutated rows are requested", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    await db
      .insert(TestTable, [
        {
          id: "1",
          name: "First",
        },
        {
          id: "2",
          name: "Second",
        },
      ])
      .returning();

    expect(queries).toHaveLength(1);
    expect(queries[0]).toContain("RETURNING *");
  });

  it("returns updated rows when the update changes a where field", async () => {
    const VersionedTable = createTable({
      id: "versioned_returning_test",
      schema: zod.object({
        id: zod.string(),
        version: zod.number(),
        name: zod.string(),
      }),
    });
    const rawDb = new BunDatabase(":memory:");
    const db = new Database({
      adaptor: new BunSqliteAdaptor({ driver: rawDb }),
    });

    await db.syncTable(VersionedTable);
    await db.insert(VersionedTable, { id: "1", version: 1, name: "first" });

    const updated = await db
      .update(VersionedTable, { version: 2, name: "second" })
      .where(VersionedTable.$id.equals("1").and(VersionedTable.$version.equals(1)))
      .returning();

    expect(updated.results).toEqual([{ id: "1", version: 2, name: "second" }]);
  });

  it("gives a multi-row insert's parsed rows without RETURNING", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    const insert = db.insert(TestTable, [
      { id: "1", name: "First" },
      { id: "2", name: "Second" },
    ]);
    expect(insert.rows).toEqual([
      { id: "1", name: "First" },
      { id: "2", name: "Second" },
    ]);
    await insert;
    expect(queries).toHaveLength(1);
    expect(queries[0]).not.toContain("RETURNING");
  });
});
