import { describe, expect, it } from "bun:test";
import * as z from "zod";
import { createTable, Database, raw } from "../src";
import MssqlAdaptor, { type MssqlDriver, type MssqlTransaction } from "../src/adaptors/mssql";
import NeonHttpAdaptor from "../src/adaptors/neon-http";
import PostgresAdaptor from "../src/adaptors/postgres";
import { TO_SQL_SYMBOL } from "../src/Statement";

const table = createTable({
  id: "items] quoted",
  schema: z.object({ id: z.string(), active: z.boolean(), date: z.date(), big: z.bigint() }),
});
const captureMssql = () => {
  const statements: string[] = [];
  const adaptor = new MssqlAdaptor({
    driver: {
      request: () => ({
        async query(text: string) {
          statements.push(text);
          return { recordset: [] };
        },
      }),
    },
  });
  return { db: new Database({ adaptor }), adaptor, statements };
};

describe("SQL Server dialect", () => {
  it("uses TOP and OFFSET/FETCH with escaped identifiers and Unicode predicates", async () => {
    const { db, statements } = captureMssql();
    await db.select(table, { id: table.$id }).where(table.$id.equals("東京'🦄")).limit(1);
    await db.select(table, { id: table.$id }).offset(2).limit(3).orderBy(table.$id, "DESC");
    await db.select(table, { id: table.$id }).offset(2);
    await db.select(table, { id: table.$id }).offset(2).limit(0);
    expect(statements[0]).toContain("SELECT TOP (1) [id] FROM [items]] quoted]");
    expect(statements[0]).toContain("N'東京''🦄'");
    expect(statements[1]).toContain("ORDER BY [id] DESC OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");
    expect(statements[2]).toContain("ORDER BY (SELECT NULL) OFFSET 2 ROWS");
    expect(statements[3]).toContain("TOP (0)");
    expect(statements[3]).not.toContain("FETCH NEXT 0");
  });
  it("places OUTPUT before VALUES and leaves scalar dates/bigints in native database types", async () => {
    const { db, statements } = captureMssql();
    await db
      .insert(table, {
        id: "new",
        active: false,
        date: db.select(table, { date: table.$date }).limit(1),
        big: db.select(table, { big: table.$big }).limit(1),
      })
      .selectMutated();
    const text = statements[0]!;
    expect(text).toContain(
      "OUTPUT INSERTED.[id], INSERTED.[active], CONVERT(NVARCHAR(30), INSERTED.[date], 126) AS [date], CONVERT(NVARCHAR(30), INSERTED.[big]) AS [big] VALUES",
    );
    expect(text).toContain("(SELECT TOP (1) [date]");
    expect(text).toContain("(SELECT TOP (1) [big]");
    expect(text).toContain("N'new', 0");
    expect(text).not.toContain("RETURNING");
    expect(statements).toHaveLength(1);
  });
  it("uses BIT literals for predicates and updates", async () => {
    const { db, statements } = captureMssql();
    await db.select(table, { id: table.$id }).where(table.$active.equals(true));
    await db.update(table, { active: false }, table.$active.equals(true)).selectMutated();
    expect(statements[0]).toMatch(/\[active\] =\s+1/);
    expect(statements[1]).toContain("SET [active] = 0 OUTPUT");
    expect(statements[1]).not.toContain("false");
  });
  it("preserves exact bigint, date, boolean and JSON return values", async () => {
    const jsonTable = createTable({ id: "typed", schema: table.schema.extend({ json: z.array(z.string()) }) });
    const adaptor = new MssqlAdaptor({
      driver: {
        request: () => ({
          async query() {
            return {
              recordset: [
                {
                  id: "x",
                  active: true,
                  date: "2026-09-16T01:02:03.456",
                  big: "9223372036854775807",
                  json: '["東京"]',
                },
              ],
            };
          },
        }),
      },
    });
    expect((await new Database({ adaptor }).select(jsonTable)).first).toEqual({
      id: "x",
      active: true,
      date: new Date("2026-09-16T01:02:03.456Z"),
      big: 9223372036854775807n,
      json: ["東京"],
    });
  });
  it("emits type-aware JSON membership and rejects object containment explicitly", () => {
    const { adaptor } = captureMssql();
    expect(adaptor.buildJsonArrayContainsSql("[data]", false)[TO_SQL_SYMBOL]()).toContain(
      "[type] = 3 AND [value] COLLATE Latin1_General_100_BIN2 = N'false'",
    );
    expect(adaptor.buildJsonArrayContainsSql("[data]", null)[TO_SQL_SYMBOL]()).toContain(
      "[type] = 0 AND [value] IS NULL",
    );
    expect(() => adaptor.buildJsonArrayContainsSql("[data]", {})).toThrow("scalar");
  });
  it("pins statements to the native transaction and preserves failures even if rollback fails", async () => {
    const events: string[] = [];
    const tx: MssqlTransaction = {
      async begin() {
        events.push("begin");
      },
      async commit() {
        events.push("commit");
      },
      async rollback() {
        events.push("rollback");
        throw new Error("already rolled back");
      },
      request: () => ({
        async query() {
          events.push("tx query");
          return {};
        },
      }),
    };
    const driver: MssqlDriver = {
      transaction: () => tx,
      request: () => {
        throw new Error("pool must not execute transaction queries");
      },
    };
    const db = new Database({ adaptor: new MssqlAdaptor({ driver }) });
    await db.transaction(async (connection) => {
      await connection.execute(raw("SELECT 1"));
    });
    expect(events).toEqual(["begin", "tx query", "commit"]);
    events.length = 0;
    await expect(
      db.transaction(async (connection) => {
        await connection.execute(raw("SELECT 1"));
        throw new Error("original");
      }),
    ).rejects.toThrow("original");
    expect(events).toEqual(["begin", "tx query", "rollback"]);
  });
});

describe("Neon HTTP", () => {
  it("requests object rows with full results and uses one request for scalar inserts", async () => {
    const calls: unknown[][] = [];
    const driver = {
      async query(...args: any[]) {
        calls.push(args);
        return { rows: [{ id: "new" }] };
      },
    };
    const db = new Database({ adaptor: new NeonHttpAdaptor({ driver }) });
    const simple = createTable({ id: "simple", schema: z.object({ id: z.string() }) });
    expect(
      (await db.insert(simple, { id: db.select(simple, { id: simple.$id }).one() }).selectMutated()).first,
    ).toEqual({
      id: "new",
    });
    expect(calls).toHaveLength(1);
    expect(calls[0]?.[0]).toContain("RETURNING");
    expect(calls[0]?.slice(1)).toEqual([[], { arrayMode: false, fullResults: true }]);
  });
  it("rejects interactive transactions before invoking the callback or driver", async () => {
    let called = false;
    const db = new Database({
      adaptor: new NeonHttpAdaptor({
        driver: {
          async query() {
            called = true;
            return { rows: [] };
          },
        },
      }),
    });
    await expect(
      db.transaction(async () => {
        called = true;
      }),
    ).rejects.toThrow("interactive transactions");
    expect(called).toBe(false);
  });
  it("reports failed requests without hiding the provider error", async () => {
    const events: boolean[] = [];
    const adaptor = new NeonHttpAdaptor({
      driver: {
        async query() {
          throw new Error("network unavailable");
        },
      },
      events: { onExecuteStatement: (event) => events.push(event.success) },
    });
    await expect(adaptor.execute(raw("SELECT 1"))).rejects.toThrow("network unavailable");
    expect(events).toEqual([false]);
  });
  it("supports structural PostgreSQL pools and releases their client after errors", async () => {
    const statements: string[] = [];
    let released = false;
    const driver = {
      totalCount: 1,
      async query() {
        throw new Error("must use pinned client");
      },
      async connect() {
        return {
          async query(text: string) {
            statements.push(text);
            return { rows: [] };
          },
          release() {
            released = true;
          },
        };
      },
    };
    const adaptor = new PostgresAdaptor({ driver });
    await expect(
      adaptor.transaction(async (connection) => {
        await connection.execute(raw("SELECT 1"));
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect(statements).toEqual(["BEGIN", "SELECT 1", "ROLLBACK"]);
    expect(released).toBe(true);
  });
});

it("SQL Server compiles joined selections and paged includes into one FOR JSON statement", async () => {
  const child = createTable({
    id: "child] table",
    schema: z.object({ id: z.string(), parentId: z.string(), at: z.date() }),
  });
  const { db, statements } = captureMssql();
  await db
    .select(table, { id: table.$id })
    .include({
      children: db.select(child).where(child.$parentId.equals(table.$id)).orderBy(child.$id, "ASC").offset(1).limit(2),
    })
    .one();
  expect(statements).toHaveLength(1);
  expect(statements[0]).toContain("SELECT TOP (1)");
  expect(statements[0]).toContain("FOR JSON PATH, INCLUDE_NULL_VALUES");
  expect(statements[0]).toContain("OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY");
  expect(statements[0]).toContain("CONVERT(NVARCHAR(30), [child]] table].[at], 126)");
  expect(statements[0]).not.toContain("LIMIT");
  statements.length = 0;
  await db.select(table, { parent: table, child }).leftJoin(child, child.$parentId.equals(table.$id));
  expect(statements).toHaveLength(1);
  expect(statements[0]).toContain("LEFT JOIN");
});
