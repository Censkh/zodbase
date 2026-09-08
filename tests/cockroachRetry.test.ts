import { describe, expect, test } from "bun:test";
import type { Client } from "pg";
import { sql } from "../src";
import CockroachAdaptor from "../src/adaptors/cockroach";

describe("Cockroach transaction retries", () => {
  for (const failureAt of ["SELECT 2", "COMMIT"]) {
    test(`replays the whole transaction after a serialization failure at ${failureAt}`, async () => {
      const statements: string[] = [];
      let failed = false;
      const adaptor = new CockroachAdaptor({
        driver: {
          query: async (statement: string) => {
            statements.push(statement);
            if (statement === failureAt && !failed) {
              failed = true;
              throw Object.assign(new Error("WriteTooOldError"), { code: "40001" });
            }
            return { rows: [] };
          },
        } as unknown as Client,
      });
      const result = await adaptor.transaction(async (transaction) => {
        await transaction.execute(sql`SELECT 1`);
        await transaction.execute(sql`SELECT 2`);
        return "saved";
      });
      expect(result).toBe("saved");
      expect(statements).toEqual([
        "BEGIN",
        "SELECT 1",
        "SELECT 2",
        ...(failureAt === "COMMIT" ? ["COMMIT"] : []),
        "ROLLBACK",
        "BEGIN",
        "SELECT 1",
        "SELECT 2",
        "COMMIT",
      ]);
    });
  }

  for (const code of ["40001", "40003", "23505"]) {
    test(`bounds retries and preserves error ${code}`, async () => {
      let attempts = 0;
      const error = Object.assign(new Error("failed"), { code });
      const adaptor = new CockroachAdaptor({
        driver: {
          query: async () => ({ rows: [] }),
        } as unknown as Client,
      });
      await expect(
        adaptor.transaction(async () => {
          attempts++;
          throw error;
        }),
      ).rejects.toBe(error);
      expect(attempts).toBe(code === "40001" ? 3 : 1);
      expect(await adaptor.transaction(async () => "next transaction")).toBe("next transaction");
    });
  }
});
