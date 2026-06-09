import * as zod from "zod/v4";
import { createTable, Database } from "../src";
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

    const result = await db.insertMany(TestTable, [
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
      .selectMutated();

    expect(queries).toHaveLength(1);
    expect(queries[0]).toContain("RETURNING *");
  });

  it("returns parsed insert rows without RETURNING when parsed rows are requested", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    const result = await db
      .insert(TestTable, {
        id: "1",
        name: "First",
      })
      .selectParsed();

    expect(queries).toHaveLength(1);
    expect(queries[0]).not.toContain("RETURNING");
    expect(result).toEqual({
      first: {
        id: "1",
        name: "First",
      },
      results: [
        {
          id: "1",
          name: "First",
        },
      ],
    });
  });

  it("uses RETURNING for insertMany when mutated rows are requested", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    await db
      .insertMany(TestTable, [
        {
          id: "1",
          name: "First",
        },
        {
          id: "2",
          name: "Second",
        },
      ])
      .selectMutated();

    expect(queries).toHaveLength(1);
    expect(queries[0]).toContain("RETURNING *");
  });

  it("returns parsed insertMany rows without RETURNING when parsed rows are requested", async () => {
    const { db, queries } = createPostgresSqlCaptureDb();

    const result = await db
      .insertMany(TestTable, [
        {
          id: "1",
          name: "First",
        },
        {
          id: "2",
          name: "Second",
        },
      ])
      .selectParsed();

    expect(queries).toHaveLength(1);
    expect(queries[0]).not.toContain("RETURNING");
    expect(result).toEqual({
      first: {
        id: "1",
        name: "First",
      },
      results: [
        {
          id: "1",
          name: "First",
        },
        {
          id: "2",
          name: "Second",
        },
      ],
    });
  });
});
