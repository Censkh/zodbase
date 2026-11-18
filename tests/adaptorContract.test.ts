import BunDatabase from "bun:sqlite";
import * as zod from "zod";
import { createTable, Database, sql } from "../src";
import BetterSqlite3Adaptor from "../src/adaptors/better-sqlite3";
import D1Adaptor from "../src/adaptors/d1";
import ExpoSQLiteAdaptor from "../src/adaptors/expo-sqlite";
import MysqlAdaptor from "../src/adaptors/mysql";
import PostgresAdaptor from "../src/adaptors/postgres";
import TursoAdaptor from "../src/adaptors/turso";

describe("adaptor execution contract", () => {
  it("decodes rows without changing driver-owned values or special column names", async () => {
    const row = Object.freeze({ id: "7", enabled: 1, data: '{"ok":true}', ["__proto__"]: "column" });
    const db = new Database({
      adaptor: new PostgresAdaptor({
        driver: {
          async query() {
            return { rows: [row] };
          },
        },
      }),
    });
    const table = createTable({
      id: "decoded_rows",
      schema: zod.object({ id: zod.number(), enabled: zod.boolean(), data: zod.object({ ok: zod.boolean() }) }),
    });
    const result = await db.select(table);
    expect(result.results).toEqual([{ id: 7, enabled: true, data: { ok: true }, ["__proto__"]: "column" }]);
    expect(result.first).toBe(result.results[0]);
    expect(Object.getPrototypeOf(result.first)).toBe(Object.prototype);
    expect(row).toEqual({ id: "7", enabled: 1, data: '{"ok":true}', ["__proto__"]: "column" });
  });

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
      first: { id: "1", json: '["a","b"]' },
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
      first: { id: "1", json: '["a","b"]' },
      results: [{ id: "1", json: '["a","b"]' }],
    });
    await adaptor.execute(sql`DELETE FROM example`);

    const Table = createTable({ id: "example", schema: zod.object({ id: zod.string(), name: zod.string() }) });
    await adaptor.executeUpdateMany(Table, [{ id: "1", name: "Ada" }], [Table.$id]);
    expect(preparedSql).toHaveLength(3);
    expect(executionMethods).toEqual(["all", "run"]);
    expect(batchedStatements[0]).toHaveLength(1);
  });

  it("commits D1 transaction writes in one atomic batch", async () => {
    const batches: string[][] = [];
    const driver = {
      prepare(statement: string) {
        return { statement };
      },
      async batch(statements: Array<{ statement: string }>) {
        batches.push(statements.map(({ statement }) => statement));
        return [];
      },
    };
    const db = new Database({ adaptor: new D1Adaptor({ driver: driver as never }) });
    const Table = createTable({ id: "example", schema: zod.object({ id: zod.string(), name: zod.string() }) });

    const result = await db.transaction(async (transaction) => {
      await transaction.delete(Table).where(Table.$id.equals("old"));
      await transaction.insert(Table, { id: "new", name: "Ada" });
      return "committed";
    });
    expect(result).toBe("committed");
    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(2);

    await db.transaction(async () => undefined);
    expect(batches).toHaveLength(1);

    await expect(
      db.transaction(async (transaction) => {
        await transaction.insert(Table, { id: "rolled-back", name: "Grace" });
        throw new Error("rollback");
      }),
    ).rejects.toThrow("rollback");
    expect(batches).toHaveLength(1);

    await expect(db.transaction(async (transaction) => transaction.select(Table))).rejects.toThrow(
      "D1 transactions support write operations only",
    );
  });

  it("keeps scalar subqueries inside D1 write batches and rejects returning before commit", async () => {
    const batches: string[][] = [];
    const db = new Database({
      adaptor: new D1Adaptor({
        driver: {
          prepare(statement: string) {
            return { statement };
          },
          async batch(statements: { statement: string }[]) {
            batches.push(statements.map((s) => s.statement));
            return [];
          },
        } as never,
      }),
    });
    const Table = createTable({ id: "source", schema: zod.object({ id: zod.string(), name: zod.string() }) });
    await db.transaction(async (transaction) => {
      await transaction.insert(Table, { id: "parent", name: "London" });
      await transaction.insert(Table, {
        id: "child",
        name: transaction.select(Table, { name: Table.$name }).where(Table.$id.equals("parent")),
      });
    });
    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(2);
    expect(batches[0]?.[1]).toMatch(/^INSERT[\s\S]*SELECT/);
    await expect(
      db.transaction(async (transaction) => {
        await transaction
          .insert(Table, { id: "rejected", name: transaction.select(Table, { name: Table.$name }).one() })
          .returning();
      }),
    ).rejects.toThrow("write operations only");
    expect(batches).toHaveLength(1);
  });

  it("propagates D1 batch failures", async () => {
    const batchError = new Error("batch failed");
    const driver = {
      prepare(statement: string) {
        return { statement };
      },
      async batch() {
        throw batchError;
      },
    };
    const db = new Database({ adaptor: new D1Adaptor({ driver: driver as never }) });
    const Table = createTable({ id: "example", schema: zod.object({ id: zod.string() }) });

    await expect(db.transaction(async (transaction) => transaction.insert(Table, { id: "1" }))).rejects.toBe(
      batchError,
    );
  });

  it("pins and releases PostgreSQL pool clients for transactions", async () => {
    const statements: string[] = [];
    let releases = 0;
    const client = {
      async query(statement: string) {
        statements.push(statement);
        return { rows: [] };
      },
      release() {
        releases += 1;
      },
    };
    const pool = {
      totalCount: 1,
      async connect() {
        return client;
      },
      async query() {
        throw new Error("pool query must not be used inside a transaction");
      },
    };
    const adaptor = new PostgresAdaptor({ driver: pool as never });

    expect(await adaptor.transaction(async (transaction) => transaction.execute(sql`SELECT 1`))).toMatchObject({
      results: [],
    });
    expect(statements).toEqual(["BEGIN", "SELECT 1", "COMMIT"]);
    expect(releases).toBe(1);
  });

  it("rolls back and releases MySQL pool connections without masking callback errors", async () => {
    const statements: string[] = [];
    let releases = 0;
    const callbackError = new Error("callback failed");
    const connection = {
      async query(statement: string) {
        statements.push(statement);
        if (statement === "ROLLBACK") {
          throw new Error("rollback failed");
        }
        return [[], []];
      },
      release() {
        releases += 1;
      },
    };
    const pool = {
      async getConnection() {
        return connection;
      },
      async query() {
        throw new Error("pool query must not be used inside a transaction");
      },
    };
    const adaptor = new MysqlAdaptor({ driver: pool as never });

    await expect(
      adaptor.transaction(async (transaction) => {
        await transaction.execute(sql`UPDATE example SET id = 1`);
        throw callbackError;
      }),
    ).rejects.toBe(callbackError);
    expect(statements).toEqual([
      "SET SESSION sql_mode = CONCAT_WS(',', @@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES')",
      "SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin",
      "BEGIN",
      "UPDATE example SET id = 1",
      "ROLLBACK",
    ]);
    expect(releases).toBe(1);
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

    expect(await adaptor.execute(sql`SELECT * FROM example`)).toMatchObject({
      first: { id: "1", json: '{"ok":true}' },
    });

    const Table = createTable({ id: "example", schema: zod.object({ id: zod.string(), name: zod.string() }) });
    await adaptor.executeUpdateMany(Table, [{ id: "1", name: "Ada" }], [Table.$id]);
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
        if (statement.includes("pragma_table_xinfo")) {
          return [{ _zodbase_table: "example", name: "id", notnull: 1, pk: 1, hidden: 0 }];
        }
        if (statement.includes("pragma_")) return [];
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
      generated: false,
    });
    expect((await adaptor.execute(sql`UPDATE example SET id = '2' RETURNING *`)).first).toEqual({ id: "1" });
    await adaptor.execute(sql`DELETE FROM example`);

    // The select, the live schema's three reads, and the update returning its row.
    expect(getAllSql).toHaveLength(5);
    expect(runSql).toHaveLength(1);
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
