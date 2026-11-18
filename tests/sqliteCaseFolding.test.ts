import { Database as BunDatabase } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import * as z from "zod";
import { createTable, Database, metaStore, primaryKey } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";
import { caseInsensitiveGlob } from "../src/adaptors/sqlite/CaseFolding";

// D1 refuses a LIKE or GLOB pattern longer than 50 bytes ("LIKE or GLOB pattern too complex").
const D1_PATTERN_LIMIT = 50;

const globMatches = (value: string, text: string) => {
  const db = new BunDatabase(":memory:");
  const row = db.query("SELECT LOWER(?1) GLOB ?2 AS matched").get(value, `*${caseInsensitiveGlob(text, false)}*`) as {
    matched: number;
  };
  return row.matched === 1;
};

describe("SQLite case-insensitive GLOB", () => {
  test("keeps an ASCII search within D1's pattern limit", () => {
    const search = "Meta-Assigned report for the launch.txt 2026";
    expect(new TextEncoder().encode(`*${caseInsensitiveGlob(search, false)}*`).length).toBeLessThanOrEqual(
      D1_PATTERN_LIMIT,
    );
    expect(globMatches(`Draft META-ASSIGNED REPORT FOR THE LAUNCH.TXT 2026`, search)).toBe(true);
  });

  test("matches case variants beyond ASCII, the Kelvin sign among them", () => {
    expect(globMatches("STRASSE ÉCOLE", "école")).toBe(true);
    expect(globMatches("ÉCOLE", "École")).toBe(true);
    expect(globMatches("Kelvin", "kelvin")).toBe(true);
    expect(globMatches("Kelvin", "Kelvin")).toBe(true);
    expect(globMatches("ÉCOLE", "ecole")).toBe(false);
  });
});

/** SQLite with D1's limit on a pattern, which a search longer than it must not reach. */
class PatternLimitedAdaptor extends BunSqliteAdaptor {
  protected override patternLengthLimit = D1_PATTERN_LIMIT;
}

describe("SQLite containsText on a database with D1's pattern limit", () => {
  const search = async (values: string[], text: string) => {
    const driver = new BunDatabase(":memory:");
    const db = new Database({ adaptor: new PatternLimitedAdaptor({ driver }) });
    const Notes = createTable({
      id: "note",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), text: z.string() }),
    });
    await db.syncTable(Notes);
    await db.insert(
      Notes,
      values.map((text, index) => ({ id: String(index), text })),
    );
    const statements: string[] = [];
    const prepare = driver.prepare.bind(driver);
    driver.prepare = ((query: string) => {
      statements.push(query);
      return prepare(query);
    }) as typeof driver.prepare;
    const found = await db
      .select(Notes, { text: Notes.$text })
      .where(Notes.$text.containsText(text))
      .orderBy(Notes.$id, "ASC");
    driver.close();
    return { found: found.results.map((row) => row.text), statements };
  };

  test("finds an ASCII search of any length, case-insensitively, every character literally", async () => {
    const text = "Quarterly report: 100% of *items* [draft]_v2? ".repeat(4);
    const { found, statements } = await search(
      [`Re: ${text.toUpperCase()}!`, text.replace("100%", "1000"), "other"],
      text,
    );
    expect(found).toEqual([`Re: ${text.toUpperCase()}!`]);
    expect(statements.some((statement) => /\bGLOB\b/.test(statement))).toBe(false);
  });

  test("folds letters beyond ASCII where the pattern fits, and still finds a longer search", async () => {
    expect((await search(["STRASSE ÉCOLE", "ecole"], "école")).found).toEqual(["STRASSE ÉCOLE"]);
    // Past the limit, ASCII letters still fold, and the others match as they are written.
    const long = "Résumé de l'école primaire, été ".repeat(3);
    const { found } = await search([`Le ${long.toUpperCase()}`, `Le ${long}`, "autre"], long);
    expect(found).toEqual([`Le ${long}`]);
  });
});
