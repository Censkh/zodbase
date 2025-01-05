import BunDatabase from "bun:sqlite";
import * as crypto from "node:crypto";
import * as zod from "zod";
import { Database, createTable, sql } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

describe("delete many", () => {
  it("test test deleting many users", async () => {
    const table = createTable({
      id: "user",
      schema: zod.object({
        id: zod.string(),
        name: zod.string(),
      }),
    });

    const rawDb = new BunDatabase(":memory:");
    const db = new Database({
      adaptor: new BunSqliteAdaptor(rawDb),
    });

    await db.syncTable(table);

    const ids = [];
    for (let i = 0; i < 10; i++) {
      const id = crypto.randomUUID();
      await db.insert(table, { id: id, name: `user${i}` });
      ids.push(id);
    }

    const getUserCount = async () => {
      const {
        first: { count },
      } = await db.execute(sql`SELECT COUNT(*) as count
                               FROM user`);
      return count;
    };
    expect(await getUserCount()).toEqual(10);

    await db.delete(table).where(table.$id.in(ids.slice(0, 5)));

    expect(await getUserCount()).toEqual(5);
  });

  it("test time based", async () => {
    const BoardTable = createTable({
      id: "board",
      schema: zod.object({
        id: zod.string(),
        ownerId: zod.string().nullable(),
        anonymousSessionId: zod.string().nullable(),
        createdAt: zod.number(),
      }),
    });
    const rawDb = new BunDatabase(":memory:");
    const db = new Database({
      adaptor: new BunSqliteAdaptor(rawDb),
    });

    await db.syncTable(BoardTable);

    const start = Date.now();

    await db.insertMany(BoardTable, [
      {
        id: crypto.randomUUID(),
        ownerId: null,
        anonymousSessionId: crypto.randomUUID(),
        createdAt: start - 10000,
      },
      {
        id: crypto.randomUUID(),
        ownerId: crypto.randomUUID(),
        anonymousSessionId: null,
        createdAt: start - 10000,
      },
      {
        id: crypto.randomUUID(),
        ownerId: crypto.randomUUID(),
        anonymousSessionId: crypto.randomUUID(),
        createdAt: start - 10000,
      },
    ]);

    await db.delete(BoardTable).where(
      BoardTable.$createdAt
        .lessThan(start - 1000)
        .and(BoardTable.$anonymousSessionId.notEquals(null))
        .and(BoardTable.$ownerId.equals(null)),
    );

    const {
      first: { count },
    } = await db.execute(sql`SELECT COUNT(*) as count
                               FROM ${BoardTable}`);

    expect(count).toBe(2);
  });
});
