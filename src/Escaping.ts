// @ts-expect-error
import internalEscapeSqlValue from "sql-escape-string";

export function escapeSqlValue(value: string): string {
  return internalEscapeSqlValue(value);
}
