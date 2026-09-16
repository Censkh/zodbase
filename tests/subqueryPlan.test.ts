import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as z from "zod";
import { createTable, metaStore, primaryKey, raw, sql, TO_SQL_SYMBOL } from "../src";
import type DatabaseAdaptor from "../src/DatabaseAdaptor";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);
describe.each(TEST_DATABASE_FACTORIES)("scalar insert plan: $name", ({ name, create }) => {
  it("uses an indexed parent lookup inside one statement", async () => {
    const context = await create();
    const { db } = context;
    const parent = createTable({
      id: "plan_parent",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), region: z.string(), owner: z.string() }),
    });
    const child = createTable({
      id: "plan_child",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), region: z.string() }),
    });
    try {
      await db.syncTable(parent);
      await db.syncTable(child);
      await db.insertMany(
        parent,
        Array.from({ length: 512 }, (_, i) => ({ id: `parent-${i}`, region: "London", owner: "owner" })),
      );
      if (name === "postgres" || name === "cockroach") await db.execute(sql`ANALYZE ${parent}`);
      const adaptor = (db as unknown as { options: { adaptor: DatabaseAdaptor } }).options.adaptor;
      const execute = adaptor.execute.bind(adaptor);
      const statements: string[] = [];
      adaptor.execute = (statement) => {
        statements.push(statement[TO_SQL_SYMBOL]());
        return execute(statement);
      };
      const region = db
        .select(parent, ["region"])
        .where(parent.$id.equals("parent-256").and(parent.$owner.equals("owner")));
      await db.insert(child, { id: "single", region });
      expect(statements).toHaveLength(1);
      const statement = statements[0]!;
      expect(statement).not.toMatch(/\b(?:CAST|LIMIT|ORDER BY|RETURNING)\b/);
      const explain = name === "bun-sqlite" || name === "turso-local" ? "EXPLAIN QUERY PLAN " : "EXPLAIN ";
      const result = await execute(raw(explain + statement));
      const plan = JSON.stringify(result.results);
      if (name === "postgres") {
        expect(plan).toMatch(/Index Scan/);
        expect(plan).toMatch(/InitPlan/);
        expect(plan).not.toMatch(/Seq Scan/);
      } else if (name === "cockroach") {
        expect(plan).toMatch(/plan_parent_pkey/);
        expect(plan).not.toMatch(/FULL SCAN/);
      } else if (name === "mysql" || name === "mariadb") {
        const lookup = result.results.find((row) => row.table === "plan_parent");
        expect(lookup?.key).toBe("PRIMARY");
        expect(lookup?.type).toBe("const");
      } else {
        expect(plan).toMatch(/SEARCH plan_parent USING INDEX/);
        expect(plan).not.toMatch(/SCAN plan_parent/);
      }
      statements.length = 0;
      await db.insertMany(child, [
        { id: "batch-1", region },
        { id: "batch-2", region },
      ]);
      expect(statements).toHaveLength(1);
      const bulkPlan = JSON.stringify((await execute(raw(explain + statements[0]!))).results);
      if (name === "postgres") {
        expect(bulkPlan.match(/Index Scan using plan_parent_pkey/g)).toHaveLength(1);
        expect(bulkPlan).toContain("CTE Scan");
      } else if (name === "cockroach") {
        expect(bulkPlan).toContain("auto commit");
        expect(bulkPlan).not.toContain("scan buffer");
        expect(bulkPlan).not.toContain("FULL SCAN");
      }
      expect((await db.select(child, ["region"])).results).toEqual(
        Array.from({ length: 3 }, () => ({ region: "London" })),
      );
      const missing = db.select(parent, ["region"]).where(parent.$id.equals("absent"));
      await expect(
        Promise.resolve(
          db.insertMany(child, [
            { id: "bad-1", region: missing },
            { id: "bad-2", region: missing },
          ]),
        ),
      ).rejects.toThrow();
      expect((await db.select(child, ["region"])).results).toHaveLength(3);
    } finally {
      await context.close();
    }
  });
});
