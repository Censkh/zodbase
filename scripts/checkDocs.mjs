// Run with Node; Bun executes the examples against an in-memory SQLite database.

import { spawnSync } from "node:child_process";
import fs from "node:fs";

const blocks = (file) =>
  [...fs.readFileSync(`website/docs/${file}.md`, "utf8").matchAll(/```ts[^\n]*\n([\s\S]*?)```/g)].map(
    (match) => match[1],
  );
const withoutImports = (code) => code.replace(/^import[\s\S]*?;\s*/gm, "");
const starter = blocks("getting-started");
const schema = starter[0].replace("export const Users", "const Users");
const connection = starter[1].split("await db.syncTable")[0].replace('import { Users } from "./tables";', "");
const source = `import assert from 'node:assert/strict';
import { foreignKey, sql, raw, TO_SQL_SYMBOL } from 'zodbase';
${schema}
${connection}
await db.syncTable(Users);
try {
${blocks("mutations")
  .filter((code) => !code.includes("ProjectLayers"))
  .map(withoutImports)
  .join("\n")}
assert.equal((await db.select(Users).where(Users.$id.equals('ada'))).first.name, 'Ada Lovelace');
assert.equal((await db.select(Users).where(Users.$id.equals('margaret'))).results.length, 0);
${blocks("transactions").map(withoutImports).join("\n")}
assert.equal((await db.select(Users).where(Users.$id.equals('temporary'))).results.length, 0);
assert.equal((await db.select(Users).where(Users.$id.equals('lin'))).first.name, 'Lin Chen');
const searchTerm = 'A';
const lastSeenId = 'ada';
${blocks("queries")
  .slice(1)
  .map((code) => `{\n${withoutImports(code)}\n}`)
  .join("\n")}
${blocks("indexes-and-relations").map(withoutImports).join("\n")}
const descending = false;
{
${blocks("sql").map(withoutImports).join("\n")}
assert.ok(text.includes('ORDER BY'));
}
console.log('Documented mutations, reads, transactions, indexes, foreign keys, and SQL examples pass.');
} finally { driver.close(); }
`;
const file = "website/.docs-check.generated.ts";
try {
  fs.writeFileSync(file, source);
  const result = spawnSync("bun", ["run", file], { stdio: "inherit" });
  if (result.error) throw result.error;
  process.exitCode = result.status || 0;
} finally {
  fs.rmSync(file, { force: true });
}
