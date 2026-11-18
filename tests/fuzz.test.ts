import fc from "fast-check";
import * as z from "zod";
import { createTable, metaStore, primaryKey, type SelectCondition } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

// Differential fuzzing: random rows and random conditions, run on every database and compared
// with a reference evaluator of what zodbase documents, in SQL's three-valued logic. A failure
// prints fast-check's seed and the smallest counterexample it shrank to. Runs use a fixed seed, so
// CI is deterministic: explore with `ZODBASE_FUZZ_SEED=random` (or rerun one with its seed) and
// `ZODBASE_FUZZ_RUNS=<n>`.
beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const RUNS = Number(process.env.ZODBASE_FUZZ_RUNS ?? 25);
const SEED_SETTING = process.env.ZODBASE_FUZZ_SEED ?? "20261118";
const SEED = SEED_SETTING === "random" ? undefined : Number(SEED_SETTING);
const settings = { numRuns: RUNS, seed: SEED, endOnFailure: false };

const createRows = () =>
  createTable({
    id: "fuzz_row",
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      text: z.string().nullable(),
      num: z.number().nullable(),
      int: z.int().nullable(),
      flag: z.boolean().nullable(),
      tags: z.array(z.string()).nullable(),
    }),
  });
type Row = z.infer<ReturnType<typeof createRows>["schema"]>;
type Column = Exclude<keyof Row, "id">;

// Values that collide often (so equality and IN hit), mixed with the characters that break naive
// quoting, LIKE escaping and case folding.
const TEXTS = [
  "",
  " ",
  "a",
  "A",
  "a ",
  "ab",
  "aB",
  "%",
  "_",
  "!",
  "*",
  "\\",
  "'",
  "''",
  '"',
  "a%b",
  "a_b",
  "\n",
  "\t",
  "é",
  "É",
  "é",
  "ß",
  "Σ",
  "σ",
  "🐍",
  "東京",
  "null",
  "NULL",
  "0",
  "$1",
  "?",
  ":name",
  "--",
  "/*",
];
const text = fc.oneof(
  { weight: 4, arbitrary: fc.constantFrom(...TEXTS) },
  { weight: 1, arbitrary: fc.string({ unit: "grapheme", maxLength: 6 }) },
);
const NUMBERS = [0, 1, -1, 0.5, -0.5, 0.1, 0.2, 0.30000000000000004, 1e-300, 1e300, -1e300];
const num = fc.oneof(
  { weight: 3, arbitrary: fc.constantFrom(...NUMBERS, Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER) },
  { weight: 1, arbitrary: fc.double({ noNaN: true, noDefaultInfinity: true }).map((n) => (Object.is(n, -0) ? 0 : n)) },
);
const int = fc.oneof(fc.constantFrom(0, 1, -1, 2147483647, -2147483648), fc.integer({ min: -5, max: 5 }));
const nullable = <T>(arbitrary: fc.Arbitrary<T>) => fc.option(arbitrary, { nil: null, freq: 4 });
const tags = fc.array(text, { maxLength: 3 });

const rowValues = fc.record({
  text: nullable(text),
  num: nullable(num),
  int: nullable(int),
  flag: nullable(fc.boolean()),
  tags: nullable(tags),
});
const rows = fc
  .array(rowValues, { maxLength: 12 })
  .map((values) => values.map((row, index): Row => ({ id: `r${index}`, ...row })));

// A condition, as data, so the reference can evaluate it and fast-check can print and shrink it.
type Leaf =
  | { op: "equals" | "notEquals"; column: Column; value: unknown }
  | {
      op: "greaterThan" | "greaterThanOrEquals" | "lessThan" | "lessThanOrEquals";
      column: "num" | "int";
      value: number;
    }
  | { op: "in" | "notIn"; column: "text" | "num" | "int"; value: unknown[] }
  | { op: "matches" | "notMatches"; column: "text"; value: string }
  | { op: "containsText"; column: "text"; value: string }
  | { op: "contains" | "notContains"; column: "tags"; value: string };
type Condition = Leaf | { op: "and" | "or"; conditions: Condition[] };

const patterns = fc.oneof(
  fc.constantFrom("*", "", "a*", "*a", "*a*", "A*", "a%*", "a_b", "*!*", "*\\*", "é*", "É*", "*σ", "Σ*"),
  text,
  text.map((value) => `${value}*`),
);
const comparison = (column: "num" | "int", values: fc.Arbitrary<number>) =>
  fc.record({
    op: fc.constantFrom("greaterThan", "greaterThanOrEquals", "lessThan", "lessThanOrEquals" as const),
    column: fc.constant(column),
    value: values,
  });
const leaf: fc.Arbitrary<Leaf> = fc.oneof(
  fc.record({
    op: fc.constantFrom("equals", "notEquals" as const),
    column: fc.constant("text" as const),
    value: nullable(text),
  }),
  fc.record({
    op: fc.constantFrom("equals", "notEquals" as const),
    column: fc.constant("num" as const),
    value: nullable(num),
  }),
  fc.record({
    op: fc.constantFrom("equals", "notEquals" as const),
    column: fc.constant("int" as const),
    value: nullable(int),
  }),
  fc.record({
    op: fc.constantFrom("equals", "notEquals" as const),
    column: fc.constant("flag" as const),
    value: nullable(fc.boolean()),
  }),
  comparison("num", num),
  comparison("int", int),
  fc.record({
    op: fc.constantFrom("in", "notIn" as const),
    column: fc.constant("text" as const),
    value: fc.array(nullable(text), { maxLength: 4 }),
  }),
  fc.record({
    op: fc.constantFrom("in", "notIn" as const),
    column: fc.constant("num" as const),
    value: fc.array(nullable(num), { maxLength: 4 }),
  }),
  fc.record({
    op: fc.constantFrom("in", "notIn" as const),
    column: fc.constant("int" as const),
    value: fc.array(nullable(int), { maxLength: 4 }),
  }),
  fc.record({
    op: fc.constantFrom("matches", "notMatches" as const),
    column: fc.constant("text" as const),
    value: patterns,
  }),
  fc.record({ op: fc.constant("containsText" as const), column: fc.constant("text" as const), value: text }),
  fc.record({
    op: fc.constantFrom("contains", "notContains" as const),
    column: fc.constant("tags" as const),
    value: text,
  }),
);
const { condition } = fc.letrec<{ condition: Condition }>((tie) => ({
  condition: fc.oneof(
    { depthSize: "small", withCrossShrink: true },
    leaf,
    fc.record({
      op: fc.constantFrom("and", "or" as const),
      conditions: fc.array(tie("condition"), { minLength: 1, maxLength: 3 }),
    }),
  ),
}));

const toCondition = (table: ReturnType<typeof createRows>, condition: Condition): SelectCondition => {
  if (condition.op === "and" || condition.op === "or") {
    const [first, ...rest] = condition.conditions.map((child) => toCondition(table, child));
    return condition.op === "and" ? first!.and(...rest) : first!.or(...rest);
  }
  const field = (table as any)[`$${condition.column}`];
  return field[condition.op](condition.value);
};

// The reference: SQL's three-valued logic, with `undefined` as UNKNOWN. A WHERE keeps a row only
// when its condition is true.
type Truth = boolean | undefined;
const not = (value: Truth): Truth => (value === undefined ? undefined : !value);
// Case-insensitive, as documented: each code point folds to its lowercase one where that is a single
// code point, as databases fold case (never `İ` to `i̇`, two code points).
const fold = (value: string) =>
  Array.from(value, (char) => {
    const lower = char.toLowerCase();
    return Array.from(lower).length === 1 ? lower : char;
  }).join("");
const glob = (pattern: string) =>
  new RegExp(
    `^${Array.from(fold(pattern), (char) => (char === "*" ? "[\\s\\S]*" : char.replace(/[.*+?^${}()|[\]\\/]/g, "\\$&"))).join("")}$`,
    "u",
  );
const evaluate = (row: Row, condition: Condition): Truth => {
  if (condition.op === "and") {
    const values = condition.conditions.map((child) => evaluate(row, child));
    return values.includes(false) ? false : values.includes(undefined) ? undefined : true;
  }
  if (condition.op === "or") {
    const values = condition.conditions.map((child) => evaluate(row, child));
    return values.includes(true) ? true : values.includes(undefined) ? undefined : false;
  }
  const value = row[condition.column] as any;
  switch (condition.op) {
    case "equals":
      if (condition.value == null) return value === null;
      return value === null ? undefined : value === condition.value;
    case "notEquals":
      if (condition.value == null) return value !== null;
      return value === null ? undefined : value !== condition.value;
    case "greaterThan":
      return value === null ? undefined : value > condition.value;
    case "greaterThanOrEquals":
      return value === null ? undefined : value >= condition.value;
    case "lessThan":
      return value === null ? undefined : value < condition.value;
    case "lessThanOrEquals":
      return value === null ? undefined : value <= condition.value;
    // A missing value in the list matches a missing one, as `equals(null)` does.
    case "in": {
      const present = condition.value.filter((item) => item !== null);
      if (value !== null) return present.includes(value);
      return present.length < condition.value.length ? true : present.length ? undefined : false;
    }
    case "notIn": {
      const present = condition.value.filter((item) => item !== null);
      if (value !== null) return !present.includes(value);
      return present.length < condition.value.length ? false : present.length ? undefined : true;
    }
    case "matches":
      return value === null ? undefined : glob(condition.value).test(fold(value));
    case "notMatches":
      return value === null ? undefined : not(glob(condition.value).test(fold(value)));
    case "containsText":
      return value === null ? undefined : fold(value).includes(fold(condition.value));
    case "contains":
      return value === null ? undefined : value.includes(condition.value);
    case "notContains":
      return value === null ? true : !value.includes(condition.value);
  }
};

describe.each(TEST_DATABASE_FACTORIES)("fuzz: $name", ({ create }) => {
  let context: TestDatabaseContext;
  let Rows: ReturnType<typeof createRows>;
  beforeEach(async () => {
    context = await create();
    Rows = createRows();
    await context.db.syncTable(Rows);
  });
  afterEach(async () => {
    await context.close();
  });

  const replaceRows = async (values: Row[]) => {
    await context.db.delete(Rows).where(Rows.$id.notEquals(null));
    if (values.length) await context.db.insert(Rows, values);
  };
  const storedIds = async (where?: SelectCondition) => {
    const query = context.db.select(Rows, { id: Rows.$id });
    if (where) query.where(where);
    return (await query).results.map((row) => row.id).sort();
  };

  it("stores every row and reads it back unchanged", async () => {
    await fc.assert(
      fc.asyncProperty(rows, async (values) => {
        await replaceRows(values);
        const stored = (await context.db.select(Rows)).results.sort((a, b) => a.id.localeCompare(b.id));
        expect(stored).toEqual([...values].sort((a, b) => a.id.localeCompare(b.id)));
      }),
      settings,
    );
  }, 600_000);

  it("selects exactly the rows a condition holds for", async () => {
    await fc.assert(
      fc.asyncProperty(rows, fc.array(condition, { minLength: 1, maxLength: 8 }), async (values, conditions) => {
        await replaceRows(values);
        for (const where of conditions) {
          const expected = values
            .filter((row) => evaluate(row, where) === true)
            .map((row) => row.id)
            .sort();
          expect({ where, ids: await storedIds(toCondition(Rows, where)) }).toEqual({ where, ids: expected });
        }
      }),
      settings,
    );
  }, 600_000);

  it("counts, updates and deletes exactly the rows a condition holds for", async () => {
    await fc.assert(
      fc.asyncProperty(rows, condition, async (values, where) => {
        await replaceRows(values);
        const matching = values
          .filter((row) => evaluate(row, where) === true)
          .map((row) => row.id)
          .sort();
        expect(await context.db.select(Rows).where(toCondition(Rows, where)).count()).toBe(matching.length);
        const updated = await context.db.update(Rows, { int: 7 }).where(toCondition(Rows, where)).returning("id");
        expect(updated.results.map((row) => row.id).sort()).toEqual(matching);
        expect(await storedIds(Rows.$int.equals(7))).toEqual(
          values
            .filter((row) => matching.includes(row.id) || row.int === 7)
            .map((row) => row.id)
            .sort(),
        );
        await replaceRows(values);
        await context.db.delete(Rows).where(toCondition(Rows, where));
        expect(await storedIds()).toEqual(
          values
            .map((row) => row.id)
            .filter((id) => !matching.includes(id))
            .sort(),
        );
      }),
      settings,
    );
  }, 600_000);

  it("orders, limits and offsets rows, NULL first ascending and last descending", async () => {
    await fc.assert(
      fc.asyncProperty(
        rows,
        fc.constantFrom("num" as const, "int" as const, "flag" as const),
        fc.constantFrom("ASC" as const, "DESC" as const),
        fc.option(fc.nat({ max: 14 }), { nil: undefined }),
        fc.option(fc.nat({ max: 14 }), { nil: undefined }),
        async (values, column, direction, limit, offset) => {
          await replaceRows(values);
          const query = context.db
            .select(Rows, { id: Rows.$id })
            .orderBy(Rows[`$${column}`] as any, direction)
            .orderBy(Rows.$id, "ASC");
          if (limit !== undefined) query.limit(limit);
          if (offset !== undefined) query.offset(offset);
          // NULL sorts as the smallest value.
          const compare = (a: unknown, b: unknown) =>
            a === b ? 0 : a === null ? -1 : b === null ? 1 : (a as number) < (b as number) ? -1 : 1;
          const sorted = [...values].sort((a, b) => {
            const order = compare(a[column], b[column]);
            const byValue = direction === "ASC" ? order : -order;
            return byValue || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
          });
          const start = offset ?? 0;
          const expected = sorted.slice(start, limit === undefined ? undefined : start + limit).map((row) => row.id);
          expect((await query).results.map((row) => row.id)).toEqual(expected);
        },
      ),
      settings,
    );
  }, 600_000);
});
