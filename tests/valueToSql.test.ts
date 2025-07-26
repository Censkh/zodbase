import { raw, TO_SQL_SYMBOL, valueToSql } from "../src";

const evilStrings = ["' OR 1=1; --", "Robert'); DROP TABLE Students;--"];

describe("valueToSql", () => {
  it("should escape plain strings", () => {
    for (const evilString of evilStrings) {
      // sql-escape-string behavior: ' -> '' and wraps in quotes
      const expected = `'${evilString.replace(/'/g, "''")}'`;
      expect(valueToSql(evilString)).toBe(expected);
    }
  });

  it("should handle numbers", () => {
    expect(valueToSql(123)).toBe("123");
    expect(valueToSql(123.45)).toBe("123.45");
    expect(valueToSql(0)).toBe("0");
  });

  it("should handle booleans", () => {
    expect(valueToSql(true)).toBe("true");
    expect(valueToSql(false)).toBe("false");
  });

  it("should handle null and undefined", () => {
    expect(valueToSql(null)).toBe("null");
    expect(valueToSql(undefined)).toBe("null");
  });

  it("should handle arrays of primitives", () => {
    const arr = [1, "hello", true, null];
    const expected = "(1, 'hello', true, null)";
    expect(valueToSql(arr)).toBe(expected);
  });

  it("should handle arrays with strings to escape", () => {
    const arr = [1, evilStrings[0]];
    const expected = `(1, '${evilStrings[0].replace(/'/g, "''")}')`;
    expect(valueToSql(arr)).toBe(expected);
  });

  it("should handle objects by JSON stringifying and escaping", () => {
    const obj = { a: 1, b: "hello" };
    const jsonString = JSON.stringify(obj);
    const escapedJson = `'${jsonString.replace(/'/g, "''")}'`;
    const expected = `(${escapedJson})`;
    expect(valueToSql(obj)).toBe(expected);
  });

  it("should handle objects with evil values", () => {
    const obj = { a: 1, b: evilStrings[0] };
    const jsonString = JSON.stringify(obj);
    const escapedJson = `'${jsonString.replace(/'/g, "''")}'`;
    const expected = `(${escapedJson})`;
    expect(valueToSql(obj)).toBe(expected);
  });

  it("should handle objects with evil keys", () => {
    const obj = { [evilStrings[0]]: 1 };
    const jsonString = JSON.stringify(obj);
    const escapedJson = `'${jsonString.replace(/'/g, "''")}'`;
    const expected = `(${escapedJson})`;
    expect(valueToSql(obj)).toBe(expected);
  });

  it("should not escape values with TO_SQL_SYMBOL", () => {
    const customSql = "CURRENT_TIMESTAMP";
    const sqlObject = { [TO_SQL_SYMBOL]: () => customSql };
    expect(valueToSql(sqlObject)).toBe(customSql);
  });

  it("should not escape raw() statements", () => {
    const rawSql = "SELECT * FROM users";
    expect(valueToSql(raw(rawSql))).toBe(rawSql);
  });

  it("should handle nested arrays by stringifying them", () => {
    const arr = [
      [1, 2],
      ["a", "b"],
    ];
    const inner1 = valueToSql([1, 2], true);
    const inner2 = valueToSql(["a", "b"], true);
    const expected = `(${inner1}, ${inner2})`;
    expect(valueToSql(arr)).toBe(expected);
  });

  it("should handle empty strings", () => {
    expect(valueToSql("")).toBe("''");
  });
});
