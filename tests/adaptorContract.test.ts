import BunDatabase from "bun:sqlite";
import * as zod from "zod/v4";
import { createTable, sql } from "../src";
import BetterSqlite3Adaptor from "../src/adaptors/better-sqlite3";
import CockroachAdaptor from "../src/adaptors/cockroach";
import D1Adaptor from "../src/adaptors/d1";
import ExpoSQLiteAdaptor from "../src/adaptors/expo-sqlite";
import TursoAdaptor from "../src/adaptors/turso";

describe("adaptor execution contract", () => {
  it("supports the synchronous better-sqlite3 driver contract", async () => {
    const database = new BunDatabase(":memory:");
    const executions: string[] = [];
    const driver = {
      prepare(statement: string) {
        const preparedStatement = database.prepare(statement);
        const reader = statement.trim().toUpperCase().startsWith("SELECT") || /\bRETURNING\b/i.test(statement);
        return {
          reader,
          all() {
            executions.push("all");
            return preparedStatement.all();
          },
          run() {
            executions.push("run");
            return preparedStatement.run();
          },
        };
      },
    };
    const adaptor = new BetterSqlite3Adaptor({ driver });

    await adaptor.execute(sql`CREATE TABLE example (id TEXT, json JSONB)`);
    await adaptor.execute(sql`INSERT INTO example (id, json) VALUES ('1', ${JSON.stringify(["a", "b"])})`);

    expect(await adaptor.execute(sql`SELECT * FROM example`)).toMatchObject({
      first: { id: "1", json: ["a", "b"] },
    });
    expect(executions).toEqual(["run", "run", "all"]);
    database.close();
  });

  it("maps D1 rows and batches updateMany statements", async () => {
    const preparedSql: string[] = [];
    const batchedStatements: unknown[][] = [];
    const executionMethods: string[] = [];
    const driver = {
      prepare(statement: string) {
        preparedSql.push(statement);
        return {
          async all() {
            executionMethods.push("all");
            return { results: [{ id: "1", json: '["a","b"]' }] };
          },
          async run() {
            executionMethods.push("run");
            return { results: [] };
          },
        };
      },
      async batch(statements: unknown[]) {
        batchedStatements.push(statements);
      },
    };
    const adaptor = new D1Adaptor({ driver: driver as never });

    expect(await adaptor.execute(sql`SELECT * FROM example`)).toMatchObject({
      first: { id: "1", json: ["a", "b"] },
      results: [{ id: "1", json: ["a", "b"] }],
    });
    await adaptor.execute(sql`DELETE FROM example`);

    const Table = createTable({ id: "example", schema: zod.object({ id: zod.string(), name: zod.string() }) });
    await adaptor.executeUpdateMany(Table, [{ id: "1", name: "Ada" }], Table.$id);
    expect(preparedSql).toHaveLength(3);
    expect(executionMethods).toEqual(["all", "run"]);
    expect(batchedStatements[0]).toHaveLength(1);
  });

  it("maps Turso rows and sends updateMany through batch", async () => {
    const executed: string[] = [];
    const batches: string[][] = [];
    const batchModes: Array<string | undefined> = [];
    const driver = {
      async execute(statement: string) {
        executed.push(statement);
        return { rows: [{ id: "1", json: '{"ok":true}' }] };
      },
      async batch(statements: string[], mode?: string) {
        batches.push(statements);
        batchModes.push(mode);
        return [];
      },
    };
    const adaptor = new TursoAdaptor({ driver: driver as never });

    expect(await adaptor.execute(sql`SELECT * FROM example`)).toMatchObject({ first: { id: "1", json: { ok: true } } });

    const Table = createTable({ id: "example", schema: zod.object({ id: zod.string(), name: zod.string() }) });
    await adaptor.executeUpdateMany(Table, [{ id: "1", name: "Ada" }], Table.$id);
    expect(executed).toEqual(["SELECT * FROM example"]);
    expect(batches[0]).toHaveLength(1);
    expect(batchModes).toEqual(["write"]);
  });

  it("uses row-returning Expo APIs for SELECT, PRAGMA, and RETURNING", async () => {
    const getAllSql: string[] = [];
    const runSql: string[] = [];
    const driver = {
      async getAllAsync(statement: string) {
        getAllSql.push(statement);
        if (statement.trim().startsWith("PRAGMA")) {
          return [{ name: "id", notnull: 1, pk: 1 }];
        }
        return [{ id: "1" }];
      },
      async runAsync(statement: string) {
        runSql.push(statement);
        return {};
      },
    };
    const adaptor = new ExpoSQLiteAdaptor({ driver: driver as never });
    const Table = createTable({ id: "example", schema: zod.object({ id: zod.string() }) });

    expect((await adaptor.execute(sql`SELECT * FROM example`)).first).toEqual({ id: "1" });
    expect((await adaptor.fetchTableColumns(Table)).first).toEqual({
      name: "id",
      type: {},
      notNull: true,
      primaryKey: true,
    });
    expect((await adaptor.execute(sql`UPDATE example SET id = '2' RETURNING *`)).first).toEqual({ id: "1" });
    await adaptor.execute(sql`DELETE FROM example`);

    expect(getAllSql).toHaveLength(3);
    expect(runSql).toHaveLength(1);
  });

  it("maps Cockroach nullability and primary-key metadata", async () => {
    const responses = [
      {
        rows: [
          { column_name: "id", is_hidden: false, is_nullable: false, column_default: null },
          { column_name: "name", is_hidden: false, is_nullable: true, column_default: null },
          { column_name: "rowid", is_hidden: true, is_nullable: false, column_default: null },
        ],
      },
      {
        rows: [
          { column_name: "id", index_name: "example_pkey", storing: false, implicit: false },
          { column_name: "name", index_name: "example_pkey", storing: true, implicit: false },
        ],
      },
      { rows: [{ constraint_name: "example_pkey", constraint_type: "PRIMARY KEY" }] },
    ];
    const driver = {
      async query() {
        return responses.shift()!;
      },
    };
    const adaptor = new CockroachAdaptor({ driver: driver as never });
    const Table = createTable({ id: "example", schema: zod.object({ id: zod.string(), name: zod.string() }) });

    expect((await adaptor.fetchTableColumns(Table)).results).toEqual([
      { name: "id", type: {}, notNull: true, hasDefault: false, isIdentity: undefined, primaryKey: true },
      { name: "name", type: {}, notNull: false, hasDefault: false, isIdentity: undefined, primaryKey: false },
    ]);
  });

  it("rejects values that are not SQL statements", async () => {
    const adaptors = [
      new D1Adaptor({ driver: {} as never }),
      new ExpoSQLiteAdaptor({ driver: {} as never }),
      new TursoAdaptor({ driver: {} as never }),
    ];

    for (const adaptor of adaptors) {
      await expect(adaptor.execute(undefined as never)).rejects.toThrow("Invalid statement");
    }
  });
});
