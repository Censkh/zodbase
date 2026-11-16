import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import * as zod from "zod";
import { createTable, metaStore, monotonicTimestamp, primaryKey, sql } from "../src";
import {
  acquireCockroachTestContainer,
  acquirePostgresTestContainer,
  releaseCockroachTestContainer,
  releasePostgresTestContainer,
  TEST_DATABASE_FACTORIES,
} from "./helpers/databaseContract";

beforeAll(async () => {
  await Promise.all([acquireCockroachTestContainer(), acquirePostgresTestContainer()]);
}, 180_000);
afterAll(async () => {
  await Promise.all([releaseCockroachTestContainer(), releasePostgresTestContainer()]);
}, 180_000);

for (const factory of TEST_DATABASE_FACTORIES.filter(({ name }) => ["postgres", "cockroach"].includes(name))) {
  describe(`${factory.name} numeric precision`, () => {
    for (const legacy of [false, true]) {
      test(`preserves millisecond revisions ${legacy ? "after migrating REAL columns" : "in new tables"}`, async () => {
        const context = await factory.create();
        try {
          const table = createTable({
            id: "numeric_revisions",
            schema: zod.object({
              id: zod.string().meta(metaStore([primaryKey()])),
              updatedAt: zod.number().meta(metaStore([monotonicTimestamp()])),
              value: zod.number().optional(),
            }),
          });
          table.addIndex("numeric_revisions_updated_at", [table.$updatedAt]);
          let previousRevision = 0;
          if (legacy) {
            await context.db.execute(
              sql`CREATE TABLE "numeric_revisions" ("id" TEXT PRIMARY KEY, "updatedAt" REAL NOT NULL, "value" REAL)`,
            );
            // A cached revision can be ahead of the server clock after legacy rounding.
            const legacyRevision = Date.now() + 86_400_000;
            await context.db.execute(sql`INSERT INTO "numeric_revisions" VALUES ('existing', ${legacyRevision}, 0.5)`);
            previousRevision = (await context.db.select(table)).first!.updatedAt;
            await context.db.execute(
              sql`CREATE INDEX "numeric_revisions_updated_at" ON "numeric_revisions" ("updatedAt")`,
            );
          }
          await context.db.syncTable(table);
          const migrated = (await context.db.select(table)).results;
          await context.db.syncTable(table);
          expect((await context.db.select(table)).results).toEqual(migrated);
          const revision = 1789021226101;
          await context.db.insert(table, { id: "upload", updatedAt: revision, value: Math.PI });
          expect((await context.db.select(table).where(table.$id.equals("upload"))).first).toEqual({
            id: "upload",
            updatedAt: revision,
            value: Math.PI,
          });
          const future = Date.now() + 100_000;
          await context.db.insert(table, { id: "pending", updatedAt: future });
          for (let delta = 1; delta <= 3; delta++) {
            const result = await context.db
              .update(table, { updatedAt: revision })
              .where(table.$id.equals("pending"))
              .returning();
            expect(result.first.updatedAt).toBe(future + delta);
          }
          if (legacy) {
            const existing = (await context.db.select(table).where(table.$id.equals("existing"))).first!;
            expect(existing.updatedAt).toBeGreaterThan(previousRevision);
            expect(existing.value).toBe(0.5);
          }
        } finally {
          await context.close();
        }
      }, 60_000);
    }

    test("reads integer milliseconds exactly from legacy REAL integer columns", async () => {
      const context = await factory.create();
      try {
        const table = createTable({
          id: "integer_timestamps",
          schema: zod.object({
            id: zod.string().meta(metaStore([primaryKey()])),
            createdAt: zod.number().int(),
          }),
        });
        table.addIndex("integer_timestamps_created_at", [table.$createdAt]);
        // Integer columns created before zod 4 integer checks were recognised are 32-bit REAL.
        await context.db.execute(
          sql`CREATE TABLE "integer_timestamps" ("id" TEXT PRIMARY KEY, "createdAt" REAL NOT NULL)`,
        );
        await context.db.execute(
          sql`CREATE INDEX "integer_timestamps_created_at" ON "integer_timestamps" ("createdAt")`,
        );
        // 2026-11-02T23:38:40.000Z, which a 32-bit float sends as 23:40:00.
        const legacyCreatedAt = 1793662720000;
        await context.db.execute(sql`INSERT INTO "integer_timestamps" VALUES ('legacy', ${legacyCreatedAt})`);

        await context.db.syncTable(table);
        await context.db.syncTable(table);

        const createdAt = 1793662720123;
        await context.db.insert(table, { id: "new", createdAt });
        expect((await context.db.select(table).where(table.$id.equals("new"))).first).toEqual({
          id: "new",
          createdAt,
        });
        if (factory.name === "cockroach") {
          // CockroachDB stores 64-bit values in REAL columns but sends them rounded to 32 bits.
          expect((await context.db.select(table).where(table.$id.equals("legacy"))).first).toEqual({
            id: "legacy",
            createdAt: legacyCreatedAt,
          });
        }
      } finally {
        await context.close();
      }
    }, 60_000);
  });
}
