import * as z from "zod";
import { add, createTable, exists, incoming, metaStore, primaryKey, values } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

// Cases the big ORMs' suites keep because they broke someone: Prisma's NUL characters, long OR
// lists and concurrent upserts, Drizzle's large inArray and bulk inserts, and the date range
// round trips of both. The fuzzer (fuzz.test.ts) covers conditions, NULL logic and ordering.
beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const createNotes = () =>
  createTable({
    id: "orm_note",
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      text: z.string().nullable(),
      at: z.date().nullable(),
      count: z.number(),
    }),
  });

describe.each(TEST_DATABASE_FACTORIES)("ORM cases: $name ($connection)", ({ name: engine, create }) => {
  let context: TestDatabaseContext;
  let Notes: ReturnType<typeof createNotes>;
  beforeEach(async () => {
    context = await create();
    Notes = createNotes();
    await context.db.syncTable(Notes);
  });
  afterEach(async () => {
    await context.close();
  });
  const note = (id: string, values: Partial<z.input<typeof Notes.schema>> = {}) => ({
    id,
    text: null,
    at: null,
    count: 0,
    ...values,
  });

  // PostgreSQL can't store a NUL in text, and libsql reads one back as the end of the text.
  const refusesNul = engine === "postgres" || engine === "turso-local";

  it.skipIf(refusesNul)("stores and finds text holding a NUL character", async () => {
    await context.db.insert(Notes, note("nul", { text: "a\u0000b'\u0000" }));
    const found = await context.db
      .select(Notes, { id: Notes.$id, text: Notes.$text })
      .where(Notes.$text.equals("a\u0000b'\u0000"));
    expect(found.results).toEqual([{ id: "nul", text: "a\u0000b'\u0000" }]);
  });

  it.skipIf(!refusesNul)("refuses text holding a NUL character where it can't be read back", async () => {
    await expect((async () => await context.db.insert(Notes, note("nul", { text: "a\u0000b" })))()).rejects.toThrow(
      "NUL character",
    );
    expect(await context.db.select(Notes).count()).toBe(0);
  });

  it("escapes LIKE's wildcards with a backslash, and refuses a pattern ending in one", async () => {
    const texts = ["100%", "1000", "a_b", "axb", "c\\d", "cd"];
    await context.db.insert(
      Notes,
      texts.map((text, index) => note(`t${index}`, { text })),
    );
    const like = async (pattern: string) =>
      (
        await context.db.select(Notes, { text: Notes.$text }).where(Notes.$text.like(pattern)).orderBy(Notes.$id, "ASC")
      ).results.map((row) => row.text);
    expect(await like("100\\%")).toEqual(["100%"]);
    expect(await like("100%")).toEqual(["100%", "1000"]);
    expect(await like("a\\_b")).toEqual(["a_b"]);
    expect(await like("a_b")).toEqual(["a_b", "axb"]);
    expect(await like("c\\\\d")).toEqual(["c\\d"]);
    await expect((async () => await like("100\\"))()).rejects.toThrow("escape character");
  });

  it("matches a condition of thousands of ORs", async () => {
    await context.db.insert(Notes, [note("n10"), note("n2999"), note("other")]);
    const [first, ...rest] = Array.from({ length: 3000 }, (_, index) => Notes.$id.equals(`n${index}`));
    const found = await context.db
      .select(Notes, { id: Notes.$id })
      .where(first!.or(...rest))
      .orderBy(Notes.$id, "ASC");
    expect(found.results.map((row) => row.id)).toEqual(["n10", "n2999"]);
  });

  it("matches IN lists of a hundred thousand values", async () => {
    await context.db.insert(Notes, [note("n7"), note("n99999"), note("other")]);
    const ids = Array.from({ length: 100_000 }, (_, index) => `n${index}`);
    expect(await context.db.select(Notes).where(Notes.$id.in(ids)).count()).toBe(2);
    expect(await context.db.select(Notes).where(Notes.$id.notIn(ids)).count()).toBe(1);
  });

  it("reads, inserts and conditionally inserts more rows than a compound select holds", async () => {
    const Rows = createTable({ id: "orm_rows", schema: z.object({ id: z.string(), count: z.number() }) });
    const many = Array.from({ length: 700 }, (_, index) => ({ id: `v${index}`, count: index }));
    const rows = values(Rows, many);
    expect((await context.db.select(rows, { id: rows.$id })).results).toHaveLength(700);
    await context.db.insert(Notes).select(context.db.select(rows, { id: rows.$id, count: rows.$count }));
    expect(await context.db.select(Notes).count()).toBe(700);
    const present = exists(context.db.select(Notes, { id: Notes.$id }).where(Notes.$id.equals("v0")));
    await context.db
      .insert(
        Notes,
        many.map(({ id, count }) => note(`c${id}`, { count })),
      )
      .where(present);
    expect(await context.db.select(Notes).count()).toBe(1400);
  });

  it("round trips dates from the first year to the last of the calendar", async () => {
    const dates = [
      "0001-01-01T00:00:00.000Z",
      "1000-01-01T00:00:00.000Z",
      "1969-12-31T23:59:59.999Z",
      "9999-12-31T23:59:59.999Z",
    ];
    await context.db.insert(
      Notes,
      dates.map((at, index) => note(`d${index}`, { at: new Date(at) })),
    );
    const stored = await context.db.select(Notes, { at: Notes.$at }).orderBy(Notes.$at, "ASC");
    expect(stored.results.map((row) => row.at?.toISOString())).toEqual(dates);
    const after = await context.db.select(Notes, { id: Notes.$id }).where(Notes.$at.greaterThan(new Date(-1)));
    expect(after.results.map((row) => row.id).sort()).toEqual(["d3"]);
  });

  it.skipIf(!["postgres", "cockroach"].includes(engine))(
    "upserts one key from many connections at once without failing, adding every increment",
    async () => {
      const peers = await Promise.all(Array.from({ length: 4 }, () => context.connect!()));
      try {
        const upsert = (db: TestDatabaseContext["db"]) =>
          db
            .insert(Notes, note("shared", { count: 1 }))
            .onConflict(Notes.$id)
            .doUpdate({ count: add(Notes.$count, incoming(Notes.$count)) });
        await Promise.all(peers.flatMap((peer) => Array.from({ length: 5 }, () => upsert(peer.db))));
        expect((await context.db.select(Notes, { id: Notes.$id, count: Notes.$count })).results).toEqual([
          { id: "shared", count: 20 },
        ]);
      } finally {
        await Promise.all(peers.map((peer) => peer.close()));
      }
    },
    60_000,
  );
});
