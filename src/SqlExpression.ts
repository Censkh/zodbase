import * as zod from "zod";
import { sql } from "./index";
import type { FieldReference } from "./QueryBuilder";
import { type Statement, TO_SQL_SYMBOL, type ToSql } from "./Statement";

export class SqlExpression<T extends zod.ZodType = zod.ZodType> implements ToSql {
  constructor(
    readonly statement: Statement,
    readonly schema: T,
  ) {}

  [TO_SQL_SYMBOL]() {
    return this.statement[TO_SQL_SYMBOL]();
  }
}

/** Trusted SQL projection with a schema for driver-value decoding. */
export const expression = <T extends zod.ZodType>(statement: Statement, schema: T) =>
  new SqlExpression(statement, schema);

/** Sum a numeric column. SQL returns NULL when there are no non-null inputs. */
export const sum = (field: FieldReference<number | null | undefined> & ToSql) =>
  expression(sql`SUM(${field.table.id}.${field})`, zod.number().nullable());

/** Use a typed literal when the SQL expression yields NULL. */
export const coalesce = <T extends zod.ZodType>(
  value: SqlExpression<zod.ZodNullable<T>>,
  fallback: zod.input<NoInfer<T>>,
) => {
  const schema = value.schema.unwrap();
  return expression(sql`COALESCE(${value}, ${schema.parse(fallback)})`, schema);
};

/** Embed a compiled single-column SELECT as a scalar value. */
export const scalar = <T extends zod.ZodType>(statement: Statement, schema: T) =>
  expression(sql`(${statement})`, schema);

type NumericOperand = number | SqlExpression<zod.ZodType<number>> | (FieldReference<number> & ToSql);

const numericOperand = (value: NumericOperand) => {
  if (typeof value === "number") return zod.number().parse(value);
  if (value instanceof SqlExpression) return value;
  return sql`${value.table.id}.${value}`;
};

/** Divide with fractional results, including when both operands are integers. */
export const divide = (dividend: NumericOperand, divisor: NumericOperand) =>
  expression(sql`(${numericOperand(dividend)} / (1.0 * ${numericOperand(divisor)}))`, zod.number());

export const subtract = (left: NumericOperand, right: NumericOperand) =>
  expression(sql`(${numericOperand(left)} - ${numericOperand(right)})`, zod.number());
