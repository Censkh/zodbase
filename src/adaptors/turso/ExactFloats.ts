import { closing, closingQuote } from "../SqlText";

// A number literal in exponent notation, outside identifiers: JavaScript writes numbers below 1e-6
// or from 1e21 this way.
const EXPONENT_LITERAL = /(?<![\w$.])(\d+(?:\.\d+)?)[eE]([+-]?\d+)/g;
// libsql's SQLite (3.45) rounds a literal correctly up to this decimal exponent; beyond it, one in
// seven comes out a unit in the last place off. SQLite 3.47 parses every literal exactly.
const EXACT_EXPONENT = 22;
// Powers of two up to 2^52 are integers SQLite and JavaScript both write exactly.
const POWER_STEP = 52;

/**
 * `value` as SQL arithmetic SQLite evaluates exactly: an integer, which SQLite reads exactly,
 * scaled by powers of two, which neither multiplying nor dividing rounds.
 */
const exactFloatSql = (value: number): string => {
  // value = mantissa × 2^exponent with an integer mantissa: doubling and halving are exact.
  let mantissa = Math.abs(value);
  let exponent = 0;
  while (!Number.isInteger(mantissa)) {
    mantissa *= 2;
    exponent -= 1;
  }
  while (mantissa > Number.MAX_SAFE_INTEGER || (mantissa % 2 === 0 && exponent < 0)) {
    mantissa /= 2;
    exponent += 1;
  }
  let sql = `CAST(${value < 0 ? "-" : ""}${mantissa} AS REAL)`;
  for (let left = Math.abs(exponent); left > 0; left -= POWER_STEP) {
    sql = `(${sql} ${exponent > 0 ? "*" : "/"} ${2 ** Math.min(left, POWER_STEP)})`;
  }
  return sql;
};

const replaceInCode = (code: string) =>
  code.replace(EXPONENT_LITERAL, (literal, _digits, exponent: string) => {
    const value = Number(literal);
    return Math.abs(Number(exponent)) <= EXACT_EXPONENT || !Number.isFinite(value) || value === 0
      ? literal
      : exactFloatSql(value);
  });

/**
 * Rewrites the number literals libsql's SQLite would round wrongly as exact arithmetic, leaving
 * strings, quoted identifiers and comments as they are.
 */
export const exactFloatLiterals = (sql: string): string => {
  if (!/\d[eE]/.test(sql)) return sql;
  let output = "";
  let code = "";
  let index = 0;
  const flush = () => {
    output += replaceInCode(code);
    code = "";
  };
  const copy = (end: number) => {
    flush();
    output += sql.slice(index, end);
    index = end;
  };
  while (index < sql.length) {
    const char = sql[index]!;
    const next = sql[index + 1];
    if (char === "'" || char === '"' || char === "`") copy(closingQuote(sql, index, char));
    else if (char === "[") copy(closing(sql, index, "]"));
    else if (char === "-" && next === "-") copy(closing(sql, index, "\n"));
    else if (char === "/" && next === "*") copy(closing(sql, index + 1, "*/"));
    else {
      code += char;
      index += 1;
    }
  }
  flush();
  return output;
};
