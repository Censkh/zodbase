import { Database as BunDatabase } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
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
