import { Database as Sqlite } from "bun:sqlite";
import { resolve } from "node:path";
import { z } from "zod";

if (!process.argv[2]) throw new Error("Usage: bun scripts/benchmark.mts <baseline-checkout>");
const roots = [resolve(process.argv[2]), resolve(import.meta.dir, "..")];
let sink = 0;
async function setup(root: string) {
  const { Database, createTable, TO_SQL_SYMBOL } = await import(`${root}/src/index.ts`);
  const { snapshotCondition } = await import(`${root}/src/Subquery.ts`);
  const { buildConditionSql } = await import(`${root}/src/QueryBuilder.ts`);
  const { default: Adaptor } = await import(`${root}/src/adaptors/bun-sqlite/index.ts`);
  const driver = new Sqlite(":memory:");
  const adaptor = new Adaptor({ driver });
  const db = new Database({ adaptor });
  const table = createTable({
    id: "items",
    schema: z.object({
      id: z.number(),
      name: z.string(),
      enabled: z.boolean(),
      date: z.date(),
      data: z.object({ tag: z.string() }),
      optional: z.string().nullable(),
    }),
  });
  await db.syncTable(table);
  const rows = Array.from({ length: 1000 }, (_, id) => ({
    id,
    name: `item ${id}`,
    enabled: id % 2 === 0,
    date: new Date("2026-01-01T00:00:00Z"),
    data: { tag: "test" },
    optional: null,
  }));
  await db.insertMany(table, rows);
  const wire = driver.prepare("SELECT * FROM items").all();
  const jobs: any = {};
  for (const size of [1, 4, 16, 64]) {
    let where = table.$id.greaterThan(-1);
    for (let i = 1; i < size; i++) where = where.and(table.$id.lessThan(2000 + i));
    jobs[`snapshot-${size}`] = () => {
      sink += Object.keys(snapshotCondition(where)).length;
    };
    jobs[`build-${size}`] = () => {
      sink += buildConditionSql(adaptor, where)[TO_SQL_SYMBOL]().length;
    };
    jobs[`select-${size}`] = async () => {
      const r = await db.select(table).where(where).limit(1);
      if (r.first.id !== 0) throw Error("result");
      sink++;
    };
  }
  jobs["decode-1000"] = () => {
    const r = adaptor.decodeResult(table, { results: wire, first: wire[0] });
    if (!(r.first.date instanceof Date) || r.results[1].enabled !== false) throw Error("decode");
    sink += r.results.length;
  };
  jobs["read-1000"] = async () => {
    const r = await db.select(table);
    if (r.results.length !== 1000 || r.first.data.tag !== "test") throw Error("read");
    sink += r.results.length;
  };
  jobs["raw-read-1000"] = () => {
    sink += driver.prepare("SELECT * FROM items").all().length;
  };
  jobs["validate-1000"] = () => {
    sink += rows.map((r) => table.schema.parse(r)).length;
  };
  jobs["schema-ddl"] = () => {
    sink += adaptor.createTableStatement(table)[TO_SQL_SYMBOL]().length;
  };
  return { jobs, close: () => driver.close() };
}
const setups = [await setup(roots[0]), await setup(roots[1])];
const data = [];
for (const name of Object.keys(setups[0].jobs)) {
  const n = name.includes("1000") ? 300 : 2000;
  const samples: any = [[], []];
  for (const s of setups) for (let i = 0; i < 10; i++) await s.jobs[name]();
  for (let round = 0; round < 6; round++)
    for (const k of round % 2 ? [1, 0] : [0, 1]) {
      const wall = performance.now(),
        cpu = process.cpuUsage();
      for (let i = 0; i < n; i++) await setups[k].jobs[name]();
      const d = process.cpuUsage(cpu);
      samples[k].push({ wall: performance.now() - wall, cpu: (d.user + d.system) / 1000 });
    }
  data.push({ name, n, samples });
  console.error(name);
}
for (const s of setups) s.close();
console.log(
  JSON.stringify(
    { bun: Bun.version, roots, variants: ["baseline", "filter-snapshot-allocation-patch"], sink, data },
    null,
    2,
  ),
);
