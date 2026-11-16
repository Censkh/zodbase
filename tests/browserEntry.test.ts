import { expect, test } from "bun:test";

test("the browser entry loads tables and their bindings without the SQL compiler", async () => {
  // The full entry refuses browsers: the browser entry must not load it, even indirectly.
  const globals = globalThis as { window?: unknown };
  globals.window = {};
  try {
    const { createTable } = await import("../src/index.browser");
    const { z } = await import("zod");
    const Users = createTable({ id: "browser_users", schema: z.object({ id: z.string() }) });
    expect(String(Users.$id.key)).toBe("id");
  } finally {
    delete globals.window;
  }
});
