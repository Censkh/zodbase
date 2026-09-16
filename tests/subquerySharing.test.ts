import { expect, test } from "bun:test";
import * as z from "zod";
import { createTable, Database, metaStore, primaryKey, raw } from "../src";
import PostgresAdaptor from "../src/adaptors/postgres";

const parent = createTable({
  id: "_zodbase_scalar_0",
  schema: z.object({
    id: z.string().meta(metaStore([primaryKey()])),
    region: z.string(),
  }),
});
const child = createTable({ id: "child", schema: parent.schema });
const capture = () => {
  const statements: string[] = [];
  const db = new Database({
    adaptor: new PostgresAdaptor({
      driver: {
        async query(text: string) {
          statements.push(text);
          return { rows: [] };
        },
      } as never,
    }),
  });
  return { db, statements };
};

test("shares equivalent point queries without shadowing real table names", async () => {
  const { db, statements } = capture();
  const point = () => db.select(parent, { region: parent.$region }).where(parent.$id.equals("owner"));
  await db.insertMany(child, [
    { id: "one", region: point() },
    { id: "two", region: point() },
  ]);
  expect(statements).toHaveLength(1);
  expect(statements[0]).toContain('WITH "_zodbase_scalar_1" AS MATERIALIZED');
  expect(statements[0]?.match(/FROM "_zodbase_scalar_0"/g)).toHaveLength(1);
});

test("keeps different keys, arbitrary expressions, ordered searches and self-reads independent", async () => {
  const { db, statements } = capture();
  const cases = [
    [
      db.select(parent, { region: parent.$region }).where(parent.$id.equals("a")),
      db.select(parent, { region: parent.$region }).where(parent.$id.equals("b")),
    ],
    Array(2).fill(db.select(parent, { region: parent.$region }).where(parent.$id.equals(raw("random()::text") as any))),
    Array(2).fill(db.select(parent, { region: parent.$region }).orderBy(parent.$id, "ASC").one()),
    Array(2).fill(db.select(child, { region: child.$region }).where(child.$id.equals("a"))),
  ];
  for (const regions of cases) {
    await db.insertMany(
      child,
      regions.map((region, i) => ({ id: String(i), region })),
    );
  }
  expect(statements).toHaveLength(4);
  for (const statement of statements) expect(statement).not.toContain("MATERIALIZED");
});
