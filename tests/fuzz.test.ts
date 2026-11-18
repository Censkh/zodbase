import fc from "fast-check";
import * as z from "zod";
import { createTable, type Database, exists, metaStore, notExists, primaryKey, type SelectCondition } from "../src";
import { buildConditionSql } from "../src/QueryBuilder";
import { SqlExpression } from "../src/SqlExpression";
import { TO_SQL_SYMBOL } from "../src/Statement";
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
      big: z.bigint().nullable(),
      flag: z.boolean().nullable(),
      tags: z.array(z.string()).nullable(),
    }),
  });
type Row = z.infer<ReturnType<typeof createRows>["schema"]>;
const createOthers = () =>
  createTable({
    id: "fuzz_other",
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      text: z.string().nullable(),
      int: z.int().nullable(),
    }),
  });
type Other = z.infer<ReturnType<typeof createOthers>["schema"]>;
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
// 64-bit integers, around where a double stops holding them exactly and at their limits.
const BIG_LIMIT = 2n ** 63n;
const big = fc.oneof(
  {
    weight: 3,
    arbitrary: fc.constantFrom(
      0n,
      1n,
      -1n,
      2n ** 53n - 1n,
      2n ** 53n,
      2n ** 53n + 1n,
      -(2n ** 53n) - 1n,
      BIG_LIMIT - 1n,
      -BIG_LIMIT,
    ),
  },
  { weight: 1, arbitrary: fc.bigInt({ min: -BIG_LIMIT, max: BIG_LIMIT - 1n }) },
);
const nullable = <T>(arbitrary: fc.Arbitrary<T>) => fc.option(arbitrary, { nil: null, freq: 4 });
const tags = fc.array(text, { maxLength: 3 });

const rowValues = fc.record({
  text: nullable(text),
  num: nullable(num),
  int: nullable(int),
  big: nullable(big),
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
      column: "num" | "int" | "big";
      value: number | bigint;
    }
  | { op: "in" | "notIn"; column: "text" | "num" | "int" | "big"; value: unknown[] }
  | { op: "matches" | "notMatches"; column: "text"; value: string }
  | { op: "containsText"; column: "text"; value: string }
  | { op: "contains" | "notContains"; column: "tags"; value: string }
  | {
      op: "columns";
      left: "num" | "int" | "big";
      compare: "equals" | "notEquals" | "lessThan" | "greaterThanOrEquals";
      right: "num" | "int" | "big";
    };
type Condition = Leaf | { op: "and" | "or"; conditions: Condition[] };

const patterns = fc.oneof(
  fc.constantFrom("*", "", "a*", "*a", "*a*", "A*", "a%*", "a_b", "*!*", "*\\*", "é*", "É*", "*σ", "Σ*"),
  text,
  text.map((value) => `${value}*`),
);
const comparison = <T extends number | bigint>(column: "num" | "int" | "big", values: fc.Arbitrary<T>) =>
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
  comparison("big", big),
  // One column compared with another in the same row. Only pairs every database compares exactly:
  // PostgreSQL and MySQL compare a 64-bit integer with a double as two doubles.
  fc
    .record({
      op: fc.constant("columns" as const),
      pair: fc.constantFrom(
        ["num", "int"] as const,
        ["int", "num"] as const,
        ["int", "big"] as const,
        ["big", "int"] as const,
        ["num", "num"] as const,
        ["big", "big"] as const,
      ),
      compare: fc.constantFrom("equals", "notEquals", "lessThan", "greaterThanOrEquals" as const),
    })
    .map(({ op, pair: [left, right], compare }) => ({ op, left, compare, right })),
  fc.record({
    op: fc.constantFrom("equals", "notEquals" as const),
    column: fc.constant("big" as const),
    value: nullable(big),
  }),
  fc.record({
    op: fc.constantFrom("in", "notIn" as const),
    column: fc.constant("big" as const),
    value: fc.array(nullable(big), { maxLength: 4 }),
  }),
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

// Conditions over the whole builder, beyond what the reference models: raw LIKE, one column compared
// with another, IN over a subquery, correlated EXISTS, and chains long enough to be balanced. The
// oracles below check each database against itself.
type OtherLeaf =
  | { op: "equals" | "notEquals"; column: "text" | "int"; value: unknown }
  | { op: "lessThan"; column: "int"; value: number };
type OpenLeaf =
  | Leaf
  | { op: "like"; column: "text"; value: string }
  | {
      op: "compareColumns";
      left: "num" | "int" | "big";
      compare: "equals" | "notEquals" | "greaterThan" | "lessThanOrEquals";
      right: "num" | "int" | "big";
    }
  | { op: "inSelect" | "notInSelect"; column: "text" | "int"; where?: OtherLeaf }
  | { op: "exists" | "notExists"; on: "text" | "int"; where?: OtherLeaf };
type OpenCondition = OpenLeaf | { op: "and" | "or"; conditions: OpenCondition[] };

const otherLeaf: fc.Arbitrary<OtherLeaf> = fc.oneof(
  fc.record({
    op: fc.constantFrom("equals", "notEquals" as const),
    column: fc.constant("text" as const),
    value: nullable(text),
  }),
  fc.record({
    op: fc.constantFrom("equals", "notEquals" as const),
    column: fc.constant("int" as const),
    value: nullable(int),
  }),
  fc.record({ op: fc.constant("lessThan" as const), column: fc.constant("int" as const), value: int }),
);
const numericColumn = fc.constantFrom("num" as const, "int" as const, "big" as const);
const openLeaf: fc.Arbitrary<OpenLeaf> = fc.oneof(
  { weight: 4, arbitrary: leaf },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constant("like" as const),
      column: fc.constant("text" as const),
      // A pattern can't end in a lone backslash, its escape character: such a one escapes it.
      value: fc
        .oneof(fc.constantFrom("%", "_", "a%", "%a", "%a%", "A%", "_b", "%!%", "\\%", "a\\_b", "\\\\%"), text)
        .map((pattern) => (/(?:^|[^\\])(?:\\\\)*\\$/.test(pattern) ? `${pattern}\\` : pattern)),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constant("compareColumns" as const),
      left: numericColumn,
      compare: fc.constantFrom("equals", "notEquals", "greaterThan", "lessThanOrEquals" as const),
      right: numericColumn,
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constantFrom("inSelect", "notInSelect" as const),
      column: fc.constantFrom("text" as const, "int" as const),
      where: fc.option(otherLeaf, { nil: undefined }),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      op: fc.constantFrom("exists", "notExists" as const),
      on: fc.constantFrom("text" as const, "int" as const),
      where: fc.option(otherLeaf, { nil: undefined }),
    }),
  },
);
const { openCondition } = fc.letrec<{ openCondition: OpenCondition }>((tie) => ({
  openCondition: fc.oneof(
    { depthSize: "small", withCrossShrink: true },
    openLeaf,
    fc.record({
      op: fc.constantFrom("and", "or" as const),
      conditions: fc.array(tie("openCondition"), { minLength: 1, maxLength: 3 }),
    }),
  ),
}));
const others = fc
  .array(fc.record({ text: nullable(text), int: nullable(int) }), { maxLength: 6 })
  .map((values) => values.map((row, index): Other => ({ id: `o${index}`, ...row })));

const toOtherCondition = (table: ReturnType<typeof createOthers>, condition: OtherLeaf): SelectCondition =>
  (table as any)[`$${condition.column}`][condition.op](condition.value);

const toOpenCondition = (
  db: Database,
  table: ReturnType<typeof createRows>,
  other: ReturnType<typeof createOthers>,
  condition: OpenCondition,
): SelectCondition => {
  const field = (name: string) => (table as any)[`$${name}`];
  switch (condition.op) {
    case "and":
    case "or": {
      const [first, ...rest] = condition.conditions.map((child) => toOpenCondition(db, table, other, child));
      return condition.op === "and" ? first!.and(...rest) : first!.or(...rest);
    }
    case "like":
      return table.$text.like(condition.value);
    case "compareColumns":
      return field(condition.left)[condition.compare](field(condition.right));
    case "inSelect":
    case "notInSelect": {
      const query = db.select(other, { [condition.column]: (other as any)[`$${condition.column}`] });
      if (condition.where) query.where(toOtherCondition(other, condition.where));
      return field(condition.column)[condition.op === "inSelect" ? "in" : "notIn"](query);
    }
    case "exists":
    case "notExists": {
      const query = db
        .select(other, { id: other.$id })
        .where((other as any)[`$${condition.on}`].equals(field(condition.on)));
      if (condition.where) query.where(toOtherCondition(other, condition.where));
      return (condition.op === "exists" ? exists : notExists)(query);
    }
    default:
      return toCondition(table, condition);
  }
};

/** Each row's condition, read the unoptimized way: 1 true, 0 false, 2 unknown, per row. */
const truthOf = (condition: SelectCondition) =>
  new SqlExpression((adaptor) => {
    const predicate = buildConditionSql(adaptor!, condition, true)[TO_SQL_SYMBOL]();
    return `CASE WHEN ${predicate} THEN 1 WHEN NOT (${predicate}) THEN 0 ELSE 2 END`;
  }, z.number());

const toCondition = (table: ReturnType<typeof createRows>, condition: Condition): SelectCondition => {
  if (condition.op === "and" || condition.op === "or") {
    const [first, ...rest] = condition.conditions.map((child) => toCondition(table, child));
    return condition.op === "and" ? first!.and(...rest) : first!.or(...rest);
  }
  if (condition.op === "columns") {
    const field = (name: string) => (table as any)[`$${name}`];
    return field(condition.left)[condition.compare](field(condition.right));
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
  if (condition.op === "columns") {
    // A null on either side is unknown, so two nulls never equal: SQL compares values, not nulls.
    const left = row[condition.left] as number | bigint | null;
    const right = row[condition.right] as number | bigint | null;
    if (left === null || right === null) return undefined;
    // A number equals a bigint only when it's that integer exactly.
    const same =
      typeof left === typeof right
        ? left === right
        : Number.isInteger(typeof left === "number" ? left : right) &&
          BigInt(left as number | bigint) === BigInt(right as number | bigint);
    if (condition.compare === "equals") return same;
    if (condition.compare === "notEquals") return !same;
    return condition.compare === "lessThan" ? left < right : left >= right;
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

describe.each(TEST_DATABASE_FACTORIES)("fuzz: $name ($connection)", ({ create }) => {
  let context: TestDatabaseContext;
  let Rows: ReturnType<typeof createRows>;
  let Others: ReturnType<typeof createOthers>;
  beforeEach(async () => {
    context = await create();
    Rows = createRows();
    Others = createOthers();
    await context.db.syncTable(Rows);
    await context.db.syncTable(Others);
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

  const replaceOthers = async (values: Other[]) => {
    await context.db.delete(Others).where(Others.$id.notEquals(null));
    if (values.length) await context.db.insert(Others, values);
  };
  const open = (condition: OpenCondition) => toOpenCondition(context.db, Rows, Others, condition);

  // NoREC and TLP (SQLancer): a WHERE the optimizer plans must keep exactly the rows the same
  // condition, read per row in a projection, says are true, and a count must agree.
  it("keeps exactly the rows a condition is true for, however it's planned", async () => {
    await fc.assert(
      fc.asyncProperty(rows, others, openCondition, async (values, otherValues, where) => {
        await replaceRows(values);
        await replaceOthers(otherValues);
        const truths = await context.db.select(Rows, { id: Rows.$id, truth: truthOf(open(where)) });
        const truthy = truths.results
          .filter((row) => Number(row.truth) === 1)
          .map((row) => row.id)
          .sort();
        expect(truths.results).toHaveLength(values.length);
        expect({ where, ids: await storedIds(open(where)) }).toEqual({ where, ids: truthy });
        expect(await context.db.select(Rows).where(open(where)).count()).toBe(truthy.length);
      }),
      settings,
    );
  }, 600_000);

  // AND keeps the rows both sides keep and OR those either does, short chains and long balanced
  // ones alike.
  it("combines conditions as the sets of rows they keep", async () => {
    await fc.assert(
      fc.asyncProperty(
        rows,
        others,
        // Short chains, and ones past the 32 terms that are written as balanced trees.
        fc.oneof(
          fc.array(openCondition, { minLength: 2, maxLength: 5 }),
          fc.array(openCondition, { minLength: 33, maxLength: 70 }),
        ),
        async (values, otherValues, conditions) => {
          await replaceRows(values);
          await replaceOthers(otherValues);
          const kept = await Promise.all(conditions.map((condition) => storedIds(open(condition))));
          const [first, ...rest] = conditions.map(open);
          const all = values.map((row) => row.id).filter((id) => kept.every((ids) => ids.includes(id)));
          const any = values.map((row) => row.id).filter((id) => kept.some((ids) => ids.includes(id)));
          expect({ conditions, ids: await storedIds(first!.and(...rest)) }).toEqual({ conditions, ids: all.sort() });
          expect({ conditions, ids: await storedIds(first!.or(...rest)) }).toEqual({ conditions, ids: any.sort() });
        },
      ),
      settings,
    );
  }, 600_000);

  // Every term of these chains decides a row, so one lost, repeated or misgrouped term shows.
  it("keeps the rows each term of a long chain names", async () => {
    await fc.assert(
      fc.asyncProperty(
        // Every id a row's, so every term that names one decides it.
        rowValues.map((row) => Array.from({ length: 12 }, (_, index): Row => ({ ...row, id: `r${index}` }))),
        // Distinct ids, rows' and missing ones, past the 32 terms a chain is written as a balanced
        // tree from: each term names a row no other term does.
        fc
          .tuple(
            fc.shuffledSubarray(Array.from({ length: 12 }, (_, index) => `r${index}`)),
            fc.integer({ min: 33, max: 60 }),
          )
          .chain(([picked, length]) => {
            const misses = Array.from({ length: Math.max(0, length - picked.length) }, (_, index) => `missing${index}`);
            return fc.shuffledSubarray([...picked, ...misses], { minLength: picked.length + misses.length });
          }),
        async (values, named) => {
          await replaceRows(values);
          const ids = values.map((row) => row.id);
          const [first, ...rest] = named.map((id) => Rows.$id.equals(id));
          const [firstOther, ...restOther] = named.map((id) => Rows.$id.notEquals(id));
          expect(await storedIds(first!.or(...rest))).toEqual(ids.filter((id) => named.includes(id)).sort());
          expect(await storedIds(firstOther!.and(...restOther))).toEqual(
            ids.filter((id) => !named.includes(id)).sort(),
          );
        },
      ),
      settings,
    );
  }, 600_000);

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
        fc.constantFrom("num" as const, "int" as const, "big" as const, "flag" as const),
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
            a === b ? 0 : a === null ? -1 : b === null ? 1 : (a as number | bigint) < (b as number | bigint) ? -1 : 1;
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
