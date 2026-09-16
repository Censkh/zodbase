import { expect, test } from "bun:test";
import { neon, neonConfig } from "@neondatabase/serverless";
import * as z from "zod";
import { createTable, Database } from "../src";
import NeonHttpAdaptor from "../src/adaptors/neon-http";

test("real Neon HTTP driver overrides array mode and decodes the wire response", async () => {
  const previous = neonConfig.fetchFunction;
  const queries: { query: string; params: unknown[] }[] = [];
  neonConfig.fetchFunction = async (_url: string, options?: RequestInit) => {
    queries.push(JSON.parse(String(options?.body)));
    return Response.json({
      command: "INSERT",
      rowCount: 1,
      fields: [
        { name: "id", dataTypeID: 25 },
        { name: "big", dataTypeID: 25 },
        { name: "date", dataTypeID: 25 },
      ],
      rows: [["new", "9223372036854775807", "2026-09-16 01:02:03.456"]],
    });
  };
  try {
    const driver = neon("postgresql://test:test@example.invalid/test", { arrayMode: true });
    const db = new Database({ adaptor: new NeonHttpAdaptor({ driver }) });
    const table = createTable({ id: "wire", schema: z.object({ id: z.string(), big: z.bigint(), date: z.date() }) });
    const result = await db
      .insert(table, {
        id: "new",
        big: db.select(table, { big: table.$big }).one(),
        date: new Date("2026-09-16T01:02:03.456Z"),
      })
      .selectMutated();
    expect(result.first).toEqual({ id: "new", big: 9223372036854775807n, date: new Date("2026-09-16T01:02:03.456Z") });
    expect(queries).toHaveLength(1);
    expect(queries[0]?.query).toContain("INSERT INTO");
    expect(queries[0]?.query).toContain('SELECT "big"');
    expect(queries[0]?.params).toEqual([]);
  } finally {
    neonConfig.fetchFunction = previous;
  }
});
