import { expect, test } from "bun:test";
import * as zod from "zod";
import { jit } from "zod-compiler/jit";
import { createTable, metaStore, primaryKey, updatedAt } from "../src";

test.each(["schema", "compact"] as const)(
  "preserves zodbase schema introspection with zod-compiler %s output",
  (output) => {
    const sourceSchema = zod
      .object({
        id: zod.string().meta(metaStore([primaryKey()])),
        name: zod.string(),
        updatedAt: zod.number().meta(metaStore([updatedAt()])),
      })
      .meta({ description: "zodbase compiler compatibility" });
    const compiledSchema = jit(sourceSchema, { eager: true, output });

    expect(compiledSchema).toBe(sourceSchema);
    expect(compiledSchema.shape).toBe(sourceSchema.shape);
    expect(compiledSchema.def).toBe(sourceSchema.def);
    expect(compiledSchema.meta()).toEqual({ description: "zodbase compiler compatibility" });

    const table = createTable({ id: `compiled_${output}`, schema: compiledSchema });
    expect(table.$id.schema).toBe(compiledSchema.shape.id);
    expect(compiledSchema.parse({ id: "1", name: "Ada", updatedAt: 1 })).toEqual({
      id: "1",
      name: "Ada",
      updatedAt: 1,
    });
    expect(() => compiledSchema.parse({ id: 1, name: "Ada", updatedAt: 1 })).toThrow();
  },
);
