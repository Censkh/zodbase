import { createClient } from "@libsql/client";
import fc from "fast-check";
import { valueToSql } from "../src";
import { exactFloatLiterals } from "../src/adaptors/turso/ExactFloats";

describe("exactFloatLiterals", () => {
  const client = createClient({ url: ":memory:" });
  afterAll(() => client.close());

  it("makes libsql read every number literal exactly", async () => {
    await fc.assert(
      fc.asyncProperty(fc.double({ noNaN: true, noDefaultInfinity: true }), async (value) => {
        const { rows } = await client.execute(
          exactFloatLiterals(`SELECT CAST(${valueToSql(value)} AS REAL) AS "value"`),
        );
        expect(rows[0]!.value).toBe(Object.is(value, -0) ? 0 : value);
      }),
      { numRuns: 2000, seed: 20261118 },
    );
  });

  it("leaves numbers in strings, quoted identifiers and comments as they are", () => {
    const sql = `SELECT '1e300' AS "1e300", [2e300], \`3e300\` -- 4e300\n/* 5e300 */`;
    expect(exactFloatLiterals(sql)).toBe(sql);
  });
});
