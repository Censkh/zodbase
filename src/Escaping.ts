// @ts-expect-error
import internalEscapeSqlValue from "sql-escape-string";

export function escapeSqlValue(value: string): string {
  return internalEscapeSqlValue(value);
}

export function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

export function quoteMysqlIdentifier(value: string): string {
  return `\`${value.replace(/`/g, "``")}\``;
}
