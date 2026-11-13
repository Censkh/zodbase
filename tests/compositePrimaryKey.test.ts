import { describe, expect, it } from "bun:test";
import * as zod from "zod";
import { createTable, metaStore, primaryKey } from "../src";
import { getPrimaryKey } from "../src/Table";

const id = () => zod.string().meta(metaStore([primaryKey()]));

describe("composite primary keys", () => {
  it("orders a key by the table's primaryKey option, and a one-column key by its marked field", () => {
    const composite = createTable({
      id: "composite_order",
      schema: zod.object({ id: id(), region: zod.string() }),
      primaryKey: ["region", "id"],
    });
    expect(getPrimaryKey(composite)).toEqual(["region", "id"]);
    expect(getPrimaryKey(createTable({ id: "single", schema: zod.object({ name: zod.string(), id: id() }) }))).toEqual([
      "id",
    ]);
    expect(getPrimaryKey(createTable({ id: "keyless", schema: zod.object({ name: zod.string() }) }))).toEqual([]);
  });

  it("refuses a key whose order or columns it can't know", () => {
    expect(() => createTable({ id: "unordered", schema: zod.object({ a: id(), b: id() }) })).toThrow(
      "give a key of more than one column in order",
    );
    expect(() =>
      createTable({ id: "unknown", schema: zod.object({ a: zod.string() }), primaryKey: ["a", "b" as "a"] }),
    ).toThrow("'b' of 'unknown' is not one of its fields");
    expect(() =>
      createTable({ id: "repeated", schema: zod.object({ a: zod.string(), b: zod.string() }), primaryKey: ["a", "a"] }),
    ).toThrow("names a column more than once");
    expect(() =>
      createTable({
        id: "outside",
        schema: zod.object({ a: id(), b: zod.string(), c: zod.string() }),
        primaryKey: ["b", "c"],
      }),
    ).toThrow("'a' of 'outside' is marked primaryKey() but isn't in its primaryKey");
  });
});
