import type * as zod from "zod";
import type { Statement } from "./Statement";

export class SqlExpression<T extends zod.ZodType = zod.ZodType> {
  constructor(
    readonly statement: Statement,
    readonly schema: T,
  ) {}
}

/** Trusted SQL projection with a schema for driver-value decoding. */
export const expression = <T extends zod.ZodType>(statement: Statement, schema: T) =>
  new SqlExpression(statement, schema);
