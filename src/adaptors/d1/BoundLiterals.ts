/** D1 rejects SQL text over 100 KB, but bound values may be up to its 2 MB row limit. */
const LITERAL_BIND_THRESHOLD = 8 * 1024;
/** D1 allows 100 bound parameters per statement. */
const MAX_BOUND_LITERALS = 100;

export type BoundSql = { sql: string; params: string[] };

/**
 * Moves long single-quoted string literals out of SQL text into `?` parameters, so large values
 * (such as JSON columns) stay within D1's statement length limit. Literals inside identifiers and
 * comments, and blob literals (`X'…'`), are left as they are.
 */
export function bindLongLiterals(sql: string, threshold = LITERAL_BIND_THRESHOLD): BoundSql {
  let output = "";
  const params: string[] = [];
  let index = 0;

  const copyUntil = (end: number) => {
    output += sql.slice(index, end);
    index = end;
  };

  while (index < sql.length) {
    const char = sql[index];
    const next = sql[index + 1];

    if (char === '"' || char === "`") {
      const end = closingQuote(sql, index, char);
      copyUntil(end);
    } else if (char === "-" && next === "-") {
      const newline = sql.indexOf("\n", index);
      copyUntil(newline === -1 ? sql.length : newline + 1);
    } else if (char === "/" && next === "*") {
      const close = sql.indexOf("*/", index + 2);
      copyUntil(close === -1 ? sql.length : close + 2);
    } else if (char === "'") {
      const end = closingQuote(sql, index, "'");
      const isBlob = /[xX]/.test(sql[index - 1] ?? "") && !/[\w$]/.test(sql[index - 2] ?? "");
      const literal = sql.slice(index, end);
      if (!isBlob && literal.length > threshold && params.length < MAX_BOUND_LITERALS) {
        params.push(literal.slice(1, -1).split("''").join("'"));
        output += "?";
        index = end;
      } else {
        copyUntil(end);
      }
    } else {
      copyUntil(index + 1);
    }
  }

  return { sql: output, params };
}

/** The index just past the quote that closes the one at `start`, treating a doubled quote as escaped. */
function closingQuote(sql: string, start: number, quote: string): number {
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
}
