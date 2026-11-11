import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { ZodError } from "zod";
import { divide, subtract } from "../src";
import {
  checkAggregates,
  checkAliasedAggregate,
  checkArithmeticProjections,
  checkMixedOperands,
  checkScalarProjection,
  checkScalarUpdate,
  divideByZero,
  seedArithmeticContract,
} from "./helpers/arithmeticContract";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

describe.each(TEST_DATABASE_FACTORIES)("arithmetic expressions: $name", ({ create, name }) => {
  let context: TestDatabaseContext;
  beforeAll(async () => {
    context = await create();
    await seedArithmeticContract(context.db);
  });
  afterAll(async () => context?.close());

  it("sums numeric rows and coalesces null or empty aggregates while preserving zero", async () => {
    await checkAggregates(context.db);
  });
  it("evaluates fractional and negative arithmetic with correct grouping", async () => {
    await checkArithmeticProjections(context.db);
  });
  if (name === "postgres" || name === "cockroach") {
    it("surfaces the server's division-by-zero error", async () => {
      await expect(Promise.resolve(divideByZero(context.db))).rejects.toMatchObject({ code: "22012" });
    });
  } else {
    it("preserves a null result from division by zero in a SELECT", async () => {
      expect((await divideByZero(context.db)).results).toEqual([{ quotient: null }]);
    });
  }
  it("keeps integer and decimal operands in order", async () => {
    await checkMixedOperands(context.db);
  });
  it("sums repeated fractional values through a quoted table alias", async () => {
    await checkAliasedAggregate(context.db);
  });
  it("coalesces a zero-row scalar projection using a Unicode string with an apostrophe", async () => {
    await checkScalarProjection(context.db);
  });
  it("updates only selected balances using a scalar aggregate", async () => {
    await checkScalarUpdate(context.db);
  });
});

it("rejects non-finite JavaScript numeric literals on either side of an expression", () => {
  for (const invalid of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    expect(() => divide(invalid, 1)).toThrow(ZodError);
    expect(() => divide(1, invalid)).toThrow(ZodError);
    expect(() => subtract(invalid, 1)).toThrow(ZodError);
    expect(() => subtract(1, invalid)).toThrow(ZodError);
  }
});
