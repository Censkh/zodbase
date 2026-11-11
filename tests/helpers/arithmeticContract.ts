import { expect } from "bun:test";
import * as z from "zod";
import {
  alias,
  coalesce,
  createTable,
  type Database,
  divide,
  expression,
  scalar,
  selectStatement,
  sql,
  subtract,
  sum,
} from "../../src";

const Charges = createTable({
  id: "arithmetic charges",
  schema: z.object({ account: z.string(), select: z.number().nullable() }),
});
const Balances = createTable({
  id: "arithmetic balances",
  schema: z.object({ account: z.string(), amount: z.number(), units: z.number().int() }),
});

export async function seedArithmeticContract(db: Database) {
  await db.syncTable(Charges);
  await db.syncTable(Balances);
  await db.insertMany(Charges, [
    { account: "a", select: 3 },
    { account: "a", select: 2 },
    { account: "a", select: null },
    { account: "b", select: null },
    { account: "b", select: null },
    { account: "c", select: -4 },
    { account: "c", select: 4 },
    { account: "duplicate", select: 1.25 },
    { account: "duplicate", select: 1.25 },
    { account: "duplicate", select: -0.5 },
  ]);
  await db.insertMany(Balances, [
    { account: "a", amount: 9, units: 2 },
    { account: "b", amount: -3, units: 2 },
    { account: "c", amount: 4, units: -2 },
  ]);
}

export async function checkAggregates(db: Database) {
  const amount = sum(Charges.$select);
  const amounts = (account: string) =>
    db.select(Charges, { total: amount, orFallback: coalesce(amount, 1.25) }).where(Charges.$account.equals(account));
  expect((await amounts("a")).results).toEqual([{ total: 5, orFallback: 5 }]);
  expect((await amounts("b")).results).toEqual([{ total: null, orFallback: 1.25 }]);
  expect((await amounts("missing")).results).toEqual([{ total: null, orFallback: 1.25 }]);
  expect((await amounts("c")).results).toEqual([{ total: 0, orFallback: 0 }]);
}

export async function checkArithmeticProjections(db: Database) {
  const amount = Balances.$amount;
  const units = Balances.$units;
  const { results } = await db
    .select(Balances, {
      account: Balances.$account,
      quotient: divide(amount, units),
      grouped: divide(subtract(1, amount), subtract(units, 1)),
      difference: subtract(amount, subtract(units, 1)),
      literal: divide(5, 4),
      rawOperand: subtract(expression(sql`2 + 3`, z.number()), expression(sql`1 + 2`, z.number())),
      third: divide(1, 3),
    })
    .orderBy(Balances.$account, "ASC");
  for (const { third } of results) expect(third).toBeCloseTo(1 / 3, 10);
  expect(results.map(({ third: _third, ...row }) => row)).toEqual([
    { account: "a", quotient: 4.5, grouped: -8, difference: 8, literal: 1.25, rawOperand: 2 },
    { account: "b", quotient: -1.5, grouped: 4, difference: -4, literal: 1.25, rawOperand: 2 },
    { account: "c", quotient: -2, grouped: 1, difference: 7, literal: 1.25, rawOperand: 2 },
  ]);
}

export async function checkMixedOperands(db: Database) {
  const { first } = await db
    .select(Balances, {
      integerOverDecimal: divide(Balances.$units, 1.25),
      decimalOverInteger: divide(1.25, Balances.$units),
      integerMinusDecimal: subtract(Balances.$units, 1.25),
      decimalMinusInteger: subtract(1.25, Balances.$units),
    })
    .where(Balances.$account.equals("a"))
    .one();
  expect(first).toEqual({
    integerOverDecimal: 1.6,
    decimalOverInteger: 0.625,
    integerMinusDecimal: 0.75,
    decimalMinusInteger: -0.75,
  });
}

export async function checkAliasedAggregate(db: Database) {
  const selected = alias(Charges, 'selected "charges`]');
  const { results } = await db
    .select(selected, { total: sum(selected.$select) })
    .where(selected.$account.equals("duplicate"));
  expect(results).toEqual([{ total: 2 }]);
}

export async function checkScalarProjection(db: Database) {
  const account = (value: string) =>
    db.compile(
      selectStatement(db.select(Balances, { account: Balances.$account }).where(Balances.$account.equals(value))),
    );
  const present = scalar(await account("c"), z.string().nullable());
  const missing = scalar(await account("missing"), z.string().nullable());
  const { results } = await db
    .select(Balances, {
      present: coalesce(present, "unused"),
      missing,
      fallback: coalesce(missing, "O'Connor – 東京"),
    })
    .where(Balances.$account.equals("a"));
  expect(results).toEqual([{ present: "c", missing: null, fallback: "O'Connor – 東京" }]);
}

export function divideByZero(db: Database) {
  return db
    .select(Balances, { quotient: divide(1, 0) })
    .where(Balances.$account.equals("a"))
    .one();
}

export async function checkScalarUpdate(db: Database) {
  const total = await db.compile(
    selectStatement(
      db.select(Charges, { amount: divide(coalesce(sum(Charges.$select), 0), 2) }).where(Charges.$account.equals("a")),
    ),
  );
  await db.update(
    Balances,
    { amount: subtract(Balances.$amount, scalar(total, z.number())) },
    Balances.$account.equals("a"),
  );
  expect((await db.select(Balances).orderBy(Balances.$account, "ASC")).results).toEqual([
    { account: "a", amount: 6.5, units: 2 },
    { account: "b", amount: -3, units: 2 },
    { account: "c", amount: 4, units: -2 },
  ]);
}
