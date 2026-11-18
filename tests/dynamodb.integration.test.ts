import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { DescribeTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { GenericContainer, type StartedTestContainer } from "testcontainers";
import { z } from "zod";
import { add, createTable, Database, incoming, metaStore, monotonicTimestamp, primaryKey, raw } from "../src";
import DynamoDbAdaptor, { ZodbaseDynamoDbError } from "../src/adaptors/dynamodb";

let container: StartedTestContainer;
let client: DynamoDBClient;

beforeAll(async () => {
  container = await new GenericContainer("amazon/dynamodb-local:3.1.0")
    .withCommand(["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"])
    .withExposedPorts(8000)
    .start();
  client = new DynamoDBClient({
    endpoint: `http://${container.getHost()}:${container.getMappedPort(8000)}`,
    region: "us-east-1",
    credentials: { accessKeyId: "local", secretAccessKey: "local" },
  });
}, 120_000);

afterAll(async () => {
  client?.destroy();
  await container?.stop();
});

let run = 0;
/** A database whose tables no other test sees: each test gets its own prefix. */
const database = (options: { allowScans?: boolean } = {}) => {
  const tablePrefix = `t${++run}-${Date.now()}-`;
  const adaptor = new DynamoDbAdaptor({ driver: client, tablePrefix, ...options });
  return { db: new Database({ adaptor }), adaptor, tablePrefix };
};

const lettersTable = () => {
  const table = createTable({
    id: "letters",
    schema: z.object({
      id: z.string().meta(metaStore([primaryKey()])),
      pairId: z.string().nullable(),
      body: z.string(),
      createdAt: z.number(),
      sentAt: z.date(),
      hearts: z.bigint(),
      loved: z.boolean(),
      tags: z.array(z.string()),
      paper: z.object({ colour: z.string() }),
      openWhen: z.string().nullable(),
    }),
  });
  return table.addIndex("letters_by_pair", [table.$pairId, table.$createdAt]);
};

const letter = (id: string, createdAt: number, overrides: Record<string, unknown> = {}) => ({
  id,
  pairId: "pair-1",
  body: `Letter ${id}`,
  createdAt,
  sentAt: new Date(Date.UTC(2026, 10, 15, 12, 0, createdAt)),
  hearts: 9007199254740993n,
  loved: false,
  tags: ["birthday"],
  paper: { colour: "blush" },
  openWhen: null,
  ...overrides,
});

describe("DynamoDB adaptor", () => {
  test("syncTable creates the table keyed by its primary key, with each index", async () => {
    const { db, tablePrefix } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.syncTable(table);

    const { Table: description } = await client.send(new DescribeTableCommand({ TableName: `${tablePrefix}letters` }));
    expect(description?.KeySchema).toEqual([{ AttributeName: "id", KeyType: "HASH" }]);
    expect(description?.GlobalSecondaryIndexes?.map((index) => [index.IndexName, index.KeySchema])).toEqual([
      [
        "letters_by_pair",
        [
          { AttributeName: "pairId", KeyType: "HASH" },
          { AttributeName: "createdAt", KeyType: "RANGE" },
        ],
      ],
    ]);
  });

  test("an index declared later is added to an existing table", async () => {
    const { db, tablePrefix } = database();
    const first = createTable({
      id: "members",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), pairId: z.string() }),
    });
    await db.syncTable(first);
    const second = createTable({ id: "members", schema: first.schema });
    second.addIndex("members_by_pair", [second.$pairId]);
    await db.syncTable(second);

    const { Table: description } = await client.send(new DescribeTableCommand({ TableName: `${tablePrefix}members` }));
    expect(description?.GlobalSecondaryIndexes?.map((index) => index.IndexName)).toEqual(["members_by_pair"]);
  });

  test("a row reads back by its key with every type intact", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    const row = letter("a", 1, { openWhen: "you miss me", loved: true });
    await db.insert(table, row);

    const { first } = await db.select(table).where(table.$id.equals("a"));
    expect(first).toEqual(row);
    expect(first?.sentAt).toBeInstanceOf(Date);
    expect(typeof first?.hearts).toBe("bigint");
  });

  test("inserting a key that exists is refused, and leaves the first row alone", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, letter("a", 1));

    const error = await db.insert(table, letter("a", 2, { body: "again" })).catch((e) => e);
    expect(error).toBeInstanceOf(ZodbaseDynamoDbError);
    expect(error.code).toBe("dynamodb/duplicate-key");
    expect((await db.select(table).where(table.$id.equals("a"))).first?.body).toBe("Letter a");
  });

  test("an index answers its partition key, sorted by its sort key, with limit and offset", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, [letter("a", 3), letter("b", 1), letter("c", 2), letter("d", 4, { pairId: "pair-2" })]);

    const newest = await db.select(table).where(table.$pairId.equals("pair-1")).orderBy(table.$createdAt, "DESC");
    expect(newest.results.map((r) => r.id)).toEqual(["a", "c", "b"]);

    const page = await db
      .select(table)
      .where(table.$pairId.equals("pair-1"))
      .orderBy(table.$createdAt, "ASC")
      .limit(1)
      .offset(1);
    expect(page.results.map((r) => r.id)).toEqual(["c"]);

    const after = await db
      .select(table)
      .where(table.$pairId.equals("pair-1").and(table.$createdAt.greaterThan(1)))
      .orderBy(table.$createdAt, "ASC");
    expect(after.results.map((r) => r.id)).toEqual(["c", "a"]);
  });

  test("conditions the key cannot answer filter what it reads", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, [
      letter("a", 1, { body: "Happy birthday", tags: ["birthday", "first"] }),
      letter("b", 2, { body: "Open on our anniversary" }),
      letter("c", 3, { body: "Happy anniversary", loved: true }),
    ]);

    const happy = await db
      .select(table)
      .where(table.$pairId.equals("pair-1").and(table.$body.like("Happy%")))
      .orderBy(table.$createdAt, "ASC");
    expect(happy.results.map((r) => r.id)).toEqual(["a", "c"]);

    const loved = await db
      .select(table)
      .where(table.$pairId.equals("pair-1").and(table.$loved.equals(true).or(table.$tags.contains("first"))))
      .orderBy(table.$createdAt, "ASC");
    expect(loved.results.map((r) => r.id)).toEqual(["a", "c"]);

    const byKeys = await db
      .select(table)
      .where(table.$id.in(["c", "a", "missing"]))
      .orderBy(table.$id, "ASC");
    expect(byKeys.results.map((r) => r.id)).toEqual(["a", "c"]);
  });

  test("a condition no key or index answers is refused unless scans are allowed", async () => {
    const strict = database();
    const table = lettersTable();
    await strict.db.syncTable(table);
    await strict.db.insert(table, letter("a", 1));
    const error = await strict.db
      .select(table)
      .where(table.$body.equals("Letter a"))
      .catch((e) => e);
    expect(error).toBeInstanceOf(ZodbaseDynamoDbError);
    expect(error.code).toBe("dynamodb/needs-index");

    const scanning = database({ allowScans: true });
    await scanning.db.syncTable(table);
    await scanning.db.insert(table, letter("a", 1));
    const found = await scanning.db.select(table).where(table.$body.equals("Letter a"));
    expect(found.results.map((r) => r.id)).toEqual(["a"]);
  });

  test("an update changes the rows it names; clearing an indexed field drops the row from that index", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, [letter("a", 1), letter("b", 2)]);

    const updated = await db
      .update(table, { loved: true })
      .where(table.$pairId.equals("pair-1").and(table.$createdAt.equals(2)))
      .returning();
    expect(updated.results.map((r) => [r.id, r.loved])).toEqual([["b", true]]);
    expect((await db.select(table).where(table.$id.equals("a"))).first?.loved).toBe(false);

    await db.update(table, { pairId: null }).where(table.$id.equals("a"));
    const stillInPair = await db.select(table).where(table.$pairId.equals("pair-1"));
    expect(stillInPair.results.map((r) => r.id)).toEqual(["b"]);
    expect((await db.select(table).where(table.$id.equals("a"))).first?.pairId).toBeNull();
  });

  test("upsert replaces by the primary key and inserts or updates by another indexed field", async () => {
    const { db } = database();
    const pairs = createTable({
      id: "pairs",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        inviteCode: z.string(),
        togetherSince: z.string(),
      }),
    });
    pairs.addIndex("pairs_by_code", [pairs.$inviteCode]);
    await db.syncTable(pairs);

    await db.insert(pairs, { id: "p1", inviteCode: "LOVE42", togetherSince: "" }).onConflict(pairs.$id).doUpdate();
    await db
      .insert(pairs, { id: "p1", inviteCode: "LOVE42", togetherSince: "2024-02-14" })
      .onConflict(pairs.$id)
      .doUpdate();
    await db
      .insert(pairs, { id: "p2", inviteCode: "ROSE99", togetherSince: "" })
      .onConflict(pairs.$inviteCode)
      .doUpdate();
    await db
      .insert(pairs, { id: "p2", inviteCode: "ROSE99", togetherSince: "2023-06-01" })
      .onConflict(pairs.$inviteCode)
      .doUpdate();

    const all = await db
      .select(pairs)
      .where(pairs.$id.in(["p1", "p2"]))
      .orderBy(pairs.$id, "ASC");
    expect(all.results).toEqual([
      { id: "p1", inviteCode: "LOVE42", togetherSince: "2024-02-14" },
      { id: "p2", inviteCode: "ROSE99", togetherSince: "2023-06-01" },
    ]);
  });

  test("a conflict skipped keeps the existing row and returns only the rows inserted", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, letter("a", 1));

    const inserted = await db
      .insert(table, [letter("a", 2, { body: "again" }), letter("b", 3)])
      .onConflict(table.$id)
      .doNothing()
      .returning();
    expect(inserted.results.map((row) => row.id)).toEqual(["b"]);
    const rows = await db
      .select(table)
      .where(table.$id.in(["a", "b"]))
      .orderBy(table.$id, "ASC");
    expect(rows.results.map((row) => [row.id, row.body])).toEqual([
      ["a", "Letter a"],
      ["b", "Letter b"],
    ]);
  });

  test("a conflict's update sets the given values, incoming(field) being the inserted row's", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, letter("a", 1));

    await db
      .insert(table, letter("a", 9, { body: "rewritten" }))
      .onConflict(table.$id)
      .doUpdate({ openWhen: incoming(table.$body) });
    const { first } = await db.select(table).where(table.$id.equals("a"));
    expect([first?.body, first?.openWhen, first?.createdAt]).toEqual(["Letter a", "rewritten", 1]);

    await db.insert(table, letter("b", 2)).onConflict(table.$id).doUpdate();
    expect((await db.select(table).where(table.$id.equals("b"))).first?.createdAt).toBe(2);
  });

  test("refuses what only SQL computes: expressions and partial conflict targets", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, letter("a", 1));

    const expression = await db
      .update(table, { createdAt: add(table.$createdAt, 1) })
      .where(table.$id.equals("a"))
      .catch((e) => e);
    expect(expression).toBeInstanceOf(ZodbaseDynamoDbError);
    expect(expression.code).toBe("dynamodb/unsupported");

    const partial = await db
      .insert(table, letter("a", 2))
      .onConflict(table.$id, { where: table.$loved.equals(true) })
      .doNothing()
      .catch((e) => e);
    expect(partial).toBeInstanceOf(ZodbaseDynamoDbError);
    expect(partial.code).toBe("dynamodb/unsupported");
    expect((await db.select(table).where(table.$id.equals("a"))).first?.createdAt).toBe(1);
  });

  test("an insert of several rows is all or nothing within a batch", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, letter("b", 1));

    const error = await db.insert(table, [letter("a", 2), letter("b", 3)]).catch((e) => e);
    expect(error.code).toBe("dynamodb/duplicate-key");
    expect((await db.select(table).where(table.$id.in(["a"]))).results).toEqual([]);
  });

  test("delete and count work through the same key and index paths", async () => {
    const { db } = database();
    const table = lettersTable();
    await db.syncTable(table);
    await db.insert(table, [letter("a", 1), letter("b", 2, { openWhen: "you can't sleep" }), letter("c", 3)]);

    const inPair = db.select(table).where(table.$pairId.equals("pair-1"));
    expect(await inPair.count()).toBe(3);
    expect(await inPair.count({ upTo: 2 })).toBe(2);
    expect(
      await db
        .select(table)
        .where(table.$pairId.equals("pair-1").and(table.$openWhen.notEquals(null)))
        .count(),
    ).toBe(1);

    await db.delete(table).where(table.$pairId.equals("pair-1").and(table.$createdAt.lessThan(3)));
    const left = await db.select(table).where(table.$pairId.equals("pair-1"));
    expect(left.results.map((r) => r.id)).toEqual(["c"]);
  });

  test("a monotonic timestamp only moves forward", async () => {
    const { db } = database();
    const notes = createTable({
      id: "notes",
      schema: z.object({
        id: z.string().meta(metaStore([primaryKey()])),
        text: z.string(),
        version: z.number().meta(metaStore([monotonicTimestamp()])),
      }),
    });
    await db.syncTable(notes);
    const future = Date.now() + 60_000;
    await db.insert(notes, { id: "n", text: "a", version: future });

    await db.update(notes, { text: "b" }).where(notes.$id.equals("n"));
    await db.update(notes, { text: "c" }).where(notes.$id.equals("n"));

    const { first } = await db.select(notes).where(notes.$id.equals("n"));
    expect(first).toEqual({ id: "n", text: "c", version: future + 2 });
  });

  test("SQL-only features are refused by name", async () => {
    const { db } = database();
    const unsupported = [
      await db.execute(raw("SELECT 1")).catch((e) => e),
      await db.transaction(async () => undefined).catch((e) => e),
    ];
    for (const error of unsupported) {
      expect(error).toBeInstanceOf(ZodbaseDynamoDbError);
      expect(error.code).toBe("dynamodb/unsupported");
    }

    const table = createTable({
      id: "codes",
      schema: z.object({ id: z.string().meta(metaStore([primaryKey()])), code: z.string() }),
    });
    table.addIndex("codes_unique", [table.$code], { unique: true });
    const unique = await db.syncTable(table).catch((e) => e);
    expect(unique.code).toBe("dynamodb/unsupported");
  });
});
