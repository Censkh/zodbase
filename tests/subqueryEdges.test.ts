import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import BunDatabase from "bun:sqlite";
import * as z from "zod";
import { createTable, Database, metaStore, primaryKey } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";
import { TO_SQL_SYMBOL } from "../src/Statement";

// Inspired by Kysely's insert dialect suite and SQLAlchemy's insert compiler suite.
// These tests execute SQL, so correct-looking SQL alone cannot satisfy the contract.
describe("scalar insert edge cases", () => {
  const source = createTable({
    id: 'source " order',
    schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), value: z.string().nullable(), rank: z.number() }),
  });
  const target = createTable({
    id: 'target " select',
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      value: z.string().nullable(),
      label: z.string().trim().default("default"),
      optional: z.string().optional(),
    }),
  });
  let driver: BunDatabase;
  let db: Database;
  let statements: string[];
  beforeEach(async () => {
    driver = new BunDatabase(":memory:");
    const adaptor = new BunSqliteAdaptor({ driver });
    statements = [];
    const execute = adaptor.execute.bind(adaptor);
    adaptor.execute = (statement) => {
      statements.push(statement[TO_SQL_SYMBOL]());
      return execute(statement);
    };
    db = new Database({ adaptor });
    await db.syncTable(source);
    await db.syncTable(target);
    await db.insertMany(source, [
      { id: "a'\\", value: "東京 🦄 ' \\ ; DROP TABLE nope; --", rank: 1 },
      { id: "b", value: "second", rank: 2 },
      { id: "null", value: null, rank: 3 },
    ]);
    statements.length = 0;
  });
  afterEach(() => driver.close());
  const projection = () => db.select(source, ["value"]).where(source.$id.equals("a'\\"));

  it("is lazy, quotes identifiers and values, and memoizes concurrent awaits", async () => {
    const mutation = db.insert(target, { id: "safe", value: projection(), label: " trimmed " });
    expect(statements).toHaveLength(0);
    await Promise.all([mutation, mutation]);
    expect(statements).toHaveLength(1);
    expect((await db.select(target, ["*"])).first).toEqual({
      id: "safe", value: "東京 🦄 ' \\ ; DROP TABLE nope; --", label: "trimmed", optional: null,
    });
  });

  it("preserves NULL for missing rows and NULL projections instead of applying a JS default", async () => {
    const nullableDefault = createTable({ id: "nullable_default", schema: z.object({ value: z.string().nullable().default("fallback") }) });
    await db.syncTable(nullableDefault);
    for (const id of ["absent", "null"]) {
      const result = await db.insert(nullableDefault, { value: db.select(source, ["value"]).where(source.$id.equals(id)) }).selectMutated();
      expect(result.first).toEqual({ value: null });
    }
  });

  it("aligns mixed bulk values by column name, including fields absent from the first row", async () => {
    const result = await db.insertMany(target, [
      { value: projection(), id: "first" },
      { optional: "present", id: "second", label: " custom ", value: "literal" },
    ]).selectMutated();
    expect(result.results).toEqual([
      { id: "first", value: "東京 🦄 ' \\ ; DROP TABLE nope; --", label: "default", optional: null },
      { id: "second", value: "literal", label: "custom", optional: "present" },
    ]);
  });

  it("honors ordered limit/offset and a zero limit", async () => {
    const query = db.select(source, ["value"]).orderBy(source.$rank, "ASC").limit(1).offset(1);
    expect((await db.insert(target, { id: "offset", value: query }).selectMutated()).first?.value).toBe("second");
    expect((await db.insert(target, { id: "zero", value: projection().limit(0) }).selectMutated()).first?.value).toBeNull();
  });

  it("snapshots reusable query builders when constructing an insert", async () => {
    const query = db.select(source, ["value"]).orderBy(source.$rank, "ASC").limit(1);
    const mutation = db.insert(target, { id: "snapshot", value: query });
    query.offset(1).fields("id").where(source.$id.equals("b"));
    expect((await mutation.selectMutated()).first?.value).toBe("東京 🦄 ' \\ ; DROP TABLE nope; --");
  });

  it("rejects invalid literals, unknown expression fields and wide projections before any SQL", () => {
    expect(() => db.insert(target, { id: 123, value: projection() } as any)).toThrow();
    expect(() => db.insert(target, { id: "x", value: "literal", unknown: projection() } as any)).toThrow("Unknown insert column");
    expect(() => db.insert(target, { id: "x", value: db.select(source, ["id", "value"]) } as any)).toThrow("exactly one column");
    expect(() => db.insert(target, { id: "x", value: db.select(source, ["*"]) } as any)).toThrow("exactly one column");
    expect(statements).toHaveLength(0);
  });

  it("rolls back the entire bulk insert if a later row violates a constraint", async () => {
    await expect(Promise.resolve(db.insertMany(target, [
      { id: "duplicate", value: projection() }, { id: "duplicate", value: "literal" },
    ]))).rejects.toThrow();
    expect((await db.select(target, ["*"])).results).toEqual([]);
  });

  it("executes the subquery on the transaction connection and rolls back both writes", async () => {
    const query = db.select(source, ["value"]).where(source.$id.equals("new"));
    await expect(db.transaction(async (tx) => {
      await tx.insert(source, { id: "new", value: "uncommitted", rank: 4 });
      expect((await tx.insert(target, { id: "tx", value: query }).selectMutated()).first?.value).toBe("uncommitted");
      throw new Error("abort");
    })).rejects.toThrow("abort");
    expect((await db.select(target, ["*"])).results).toEqual([]);
    expect((await query).results).toEqual([]);
  });

  it("does not retry a failed insert when its promise is awaited again", async () => {
    await db.insert(target, { id: "duplicate", value: "existing" });
    statements.length = 0;
    const mutation = db.insert(target, { id: "duplicate", value: projection() });
    await expect(Promise.resolve(mutation)).rejects.toThrow();
    await expect(Promise.resolve(mutation)).rejects.toThrow();
    expect(statements).toHaveLength(1);
  });
});
