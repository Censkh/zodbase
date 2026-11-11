import * as zod from "zod";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import { quoteIdentifier } from "./Escaping";
import { sql, valueToSql } from "./index";
import type { FieldReference } from "./QueryBuilder";
import { type Statement, TO_SQL_SYMBOL, type ToSql } from "./Statement";

export class SqlExpression<T extends zod.ZodType = zod.ZodType> implements ToSql {
  constructor(
    private readonly source: Statement | ((adaptor?: DatabaseAdaptor) => string),
    readonly schema: T,
  ) {}

  get statement(): Statement {
    return typeof this.source === "function" ? this : this.source;
  }

  toSql(adaptor?: DatabaseAdaptor): string {
    return typeof this.source === "function" ? this.source(adaptor) : this.source[TO_SQL_SYMBOL]();
  }

  [TO_SQL_SYMBOL]() {
    return this.toSql();
  }
}

/** Trusted SQL projection with a schema for driver-value decoding. */
export const expression = <T extends zod.ZodType>(statement: Statement, schema: T) =>
  new SqlExpression(statement, schema);

const qualifiedField = (field: FieldReference<unknown>, adaptor?: DatabaseAdaptor) => {
  const quote = (name: string) => (adaptor ? adaptor.quoteIdentifier(name) : quoteIdentifier(name));
  return `${quote(String(field.table.id))}.${quote(String(field.key))}`;
};

/** Sum a numeric column. SQL returns NULL when there are no non-null inputs. */
export const sum = (field: FieldReference<number | null | undefined> & ToSql) =>
  new SqlExpression((adaptor) => `SUM(${qualifiedField(field, adaptor)})`, zod.number().nullable());

/** Use a typed literal when the SQL expression yields NULL. */
export const coalesce = <T extends zod.ZodType>(
  value: SqlExpression<zod.ZodNullable<T>>,
  fallback: zod.input<NoInfer<T>>,
) => {
  const schema = value.schema.unwrap();
  const literal = schema.parse(fallback);
  return new SqlExpression((adaptor) => {
    const fallbackSql = adaptor ? adaptor.valueToSql(literal) : valueToSql(literal);
    return `COALESCE(${value.toSql(adaptor)}, ${fallbackSql})`;
  }, schema);
};

/** Embed a compiled single-column SELECT as a scalar value. */
export const scalar = <T extends zod.ZodType>(statement: Statement, schema: T) =>
  expression(sql`(${statement})`, schema);

type NumericOperand = number | SqlExpression<zod.ZodType<number>> | (FieldReference<number> & ToSql);

const numericOperand = (value: NumericOperand): ((adaptor?: DatabaseAdaptor) => string) => {
  if (typeof value === "number") {
    const literal = zod.number().parse(value).toString();
    return () => literal;
  }
  if (value instanceof SqlExpression) return (adaptor) => `(${value.toSql(adaptor)})`;
  return (adaptor) => qualifiedField(value, adaptor);
};

// An exponent literal avoids integer division and fixed-scale rounding on MySQL and MariaDB.
export const divideSql = (dividend: string, divisor: string): string => `(${dividend} / (1.0e0 * ${divisor}))`;

/**
 * Divide with fractional results, including when both operands are integers.
 * Callers must supply a nonzero divisor for a numeric result.
 */
export const divide = (dividend: NumericOperand, divisor: NumericOperand) => {
  const left = numericOperand(dividend);
  const right = numericOperand(divisor);
  return new SqlExpression((adaptor) => {
    const numerator = left(adaptor);
    const denominator = right(adaptor);
    return adaptor ? adaptor.divideSql(numerator, denominator) : divideSql(numerator, denominator);
  }, zod.number());
};

export const subtract = (left: NumericOperand, right: NumericOperand) => {
  const minuend = numericOperand(left);
  const subtrahend = numericOperand(right);
  return new SqlExpression((adaptor) => {
    const leftSql = minuend(adaptor);
    const rightSql = subtrahend(adaptor);
    return adaptor ? adaptor.subtractSql(leftSql, rightSql) : `(${leftSql} - ${rightSql})`;
  }, zod.number());
};
