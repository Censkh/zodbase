import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as z from "zod";
import { createTable, Database, metaStore, primaryKey } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

describe.each(TEST_DATABASE_FACTORIES)("scalar insert: $name", ({ name, create }) => {
  it("resolves parents inside one insert, preserves defaults and returns database values", async () => {
    const context = await create();
    const db = context.db;
    const parent = createTable({
      id: "parent",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), region: z.string(), timestamp: z.date() }),
    });
    const child = createTable({
      id: "child",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        region: z.string(),
        label: z.string().default("default"),
        timestamp: z.date(),
      }),
    });
    try {
      await db.syncTable(parent);
      await db.syncTable(child);
      const timestamp = new Date("2026-09-16T00:00:00.000Z");
      await db.insert(parent, { id: "parent'1", region: "London", timestamp });
      const region = db.select(parent, ["region"]).where(parent.$id.equals("parent'1"));
      const date = db.select(parent, ["timestamp"]).where(parent.$id.equals("parent'1"));
      const adaptor = (db as any).options.adaptor;
      const originalExecute = adaptor.execute.bind(adaptor);
      const statements: string[] = [];
      const { TO_SQL_SYMBOL } = await import("../src/Statement");
      adaptor.execute = (statement: any) => {
        statements.push(statement[TO_SQL_SYMBOL]());
        return originalExecute(statement);
      };
      const mutation = db.insert(child, { id: "1", region, timestamp: date });
      const mysql = name === "mysql" || name === "mariadb";
      if (mysql) await mutation;
      else
        expect((await mutation.selectMutated()).first).toEqual({
          id: "1",
          region: "London",
          timestamp,
          label: "default",
        });
      expect(statements).toHaveLength(1);
      expect(statements[0]).toMatch(/^INSERT /);
      expect(statements[0]).toContain("SELECT");
      expect((await db.select(child, ["*"])).first).toEqual({ id: "1", region: "London", timestamp, label: "default" });
      const bulk = db.insertMany(child, [
        { id: "2", region, timestamp: date },
        { id: "3", region: "Singapore", timestamp },
        { id: "4", region, timestamp: date },
      ]);
      if (mysql) await bulk;
      else {
        const returned = await bulk.selectMutated();
        expect(returned.results.map((row) => row.timestamp)).toEqual([timestamp, timestamp, timestamp]);
        expect(returned.results.map((row) => row.region)).toEqual(["London", "Singapore", "London"]);
      }
      expect((await db.select(child, ["*"])).results).toHaveLength(4);
      await expect(
        Promise.resolve(
          db.insert(child, {
            id: "missing",
            region: db.select(parent, ["region"]).where(parent.$id.equals("absent")),
            timestamp,
          }),
        ),
      ).rejects.toThrow();
      expect(() => db.insert(child, { id: "bad", region: db.select(parent, ["*"]) as any, timestamp })).toThrow(
        "exactly one column",
      );
      if (mysql) {
        const before = statements.length;
        await expect(db.insert(child, { id: "no-returning", region, timestamp }).selectMutated()).rejects.toThrow(
          "RETURNING",
        );
        expect(statements).toHaveLength(before);
      }
      // A lazy connection still compiles the subquery on the resolved adaptor.
      const lazy = new Database({ adaptor: async () => adaptor });
      await lazy.insert(child, {
        id: "lazy",
        region: lazy.select(parent, ["region"]).where(parent.$id.equals("parent'1")),
        timestamp,
      });
    } finally {
      await context.close();
    }
  });
});
