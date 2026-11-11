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
