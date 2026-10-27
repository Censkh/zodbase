import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { bindLongLiterals } from "../src/adaptors/d1/BoundLiterals";
import { escapeSqlValue } from "../src/Escaping";

const storedValue = (sql: string, params: string[] = []) => {
  const db = new Database(":memory:");
  db.run(`CREATE TABLE "it's" ("value" TEXT, "blob" BLOB)`);
  db.prepare(sql).run(...params);
  return db.query(`SELECT "value", hex("blob") AS "blob" FROM "it's"`).get();
};

describe("bindLongLiterals", () => {
  test("binds a long value and stores exactly what the inline SQL stores", () => {
    const value = `{"text":"it's a \\ back\nslash ${"x".repeat(200)}"}`;
    const sql = `INSERT INTO "it's" ("value", "blob") VALUES (${escapeSqlValue(value)}, X'00ff')`;

    const bound = bindLongLiterals(sql, 100);

    expect(bound.params).toEqual([value]);
    expect(bound.sql).toBe(`INSERT INTO "it's" ("value", "blob") VALUES (?, X'00ff')`);
    expect(storedValue(bound.sql, bound.params)).toEqual(storedValue(sql));
  });

  test("leaves short literals, identifiers, comments and blob literals in the SQL", () => {
    const sql = `-- it's 'x'\nINSERT INTO "it's" /* 'long ${"y".repeat(200)}' */ ("value", "blob") VALUES ('short', X'${"ab".repeat(100)}')`;

    expect(bindLongLiterals(sql, 100)).toEqual({ sql, params: [] });
  });

  test("binds at most 100 literals, which is D1's parameter limit", () => {
    const literal = escapeSqlValue("z".repeat(20));
    const sql = `SELECT ${Array.from({ length: 105 }, () => literal).join(", ")}`;

    const bound = bindLongLiterals(sql, 10);

    expect(bound.params).toHaveLength(100);
    expect(bound.sql.match(/\?/g)).toHaveLength(100);
  });
});
