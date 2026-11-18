/** The index just past the first `end` after `start`, or the text's end. */
export const closing = (sql: string, start: number, end: string) => {
  const found = sql.indexOf(end, start + 1);
  return found === -1 ? sql.length : found + end.length;
};

/** The index just past the quote that closes the one at `start`, treating a doubled quote as escaped. */
export const closingQuote = (sql: string, start: number, quote: string): number => {
  let index = start + 1;
  while (index < sql.length) {
    if (sql[index] === quote) {
      if (sql[index + 1] === quote) {
        index += 2;
        continue;
      }
      return index + 1;
    }
    index++;
  }
  return sql.length;
};

/**
 * Writes each NUL character in a string literal as `nul`, the database's expression for one,
 * joined to the rest of the literal: SQLite and CockroachDB read a statement's text only up to a
 * NUL, but store one in a string.
 */
export const spliceNulCharacters = (sql: string, nul: string): string => {
  if (!sql.includes("\0")) return sql;
  let output = "";
  let index = 0;
  while (index < sql.length) {
    const char = sql[index]!;
    const end =
      char === "'" || char === '"' || char === "`"
        ? closingQuote(sql, index, char)
        : char === "-" && sql[index + 1] === "-"
          ? closing(sql, index, "\n")
          : char === "/" && sql[index + 1] === "*"
            ? closing(sql, index + 1, "*/")
            : index + 1;
    const text = sql.slice(index, end);
    output +=
      char === "'" && text.includes("\0")
        ? `(${text
            .slice(1, -1)
            .split("\0")
            .map((part) => `'${part}'`)
            .join(` || ${nul} || `)})`
        : text;
    index = end;
  }
  return output;
};
