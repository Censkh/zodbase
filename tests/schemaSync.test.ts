import * as z from "zod";
import { cockroachLocality, cockroachRegion } from "zodbase/adaptors/cockroach";
import {
  computed,
  createTable,
  type DatabaseAdaptor,
  foreignKey,
  metaStore,
  primaryKey,
  raw,
  sql,
  type Table,
} from "../src";
import { TO_SQL_SYMBOL } from "../src/Statement";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

// syncTables reads every table it syncs at once (fetchLiveSchema), and then writes only what
// differs. These check that the read and the per-table read agree, that a current schema costs the
// read alone, and that each kind of change syncs exactly the tables it touches, once.
beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const id = () => z.string().meta(metaStore([primaryKey()]));

/** The tables of one release of a schema; `change` picks a release that differs in one way. */
const createSchema = (change?: string) => {
  const Owners = createTable({
    id: "sync_owner",
    schema: z.object({ id: id(), name: z.string() }),
  });
  const ownerId = z.string().meta(metaStore([foreignKey({ field: Owners.$id, onDelete: "cascade" })]));
  const Items = createTable({
    id: "sync_item",
    schema: z.object({
      id: id(),
      ownerId: change === "foreign key" ? z.string() : ownerId,
      name: change === "nullability" ? z.string().nullable() : z.string(),
      status: z.string().nullable(),
      ...(change === "column added" ? { colour: z.string().nullable() } : {}),
      ...(change === "column removed" ? {} : { rank: z.number().nullable() }),
      ...(change === "computed column"
        ? { label: z.string().meta(metaStore([computed({ sql: (column) => `${column("name")} || '!'` })])) }
        : {}),
    }),
  });
  Items.addIndex(
    "sync_item_name",
    change === "index columns" ? [Items.$ownerId, Items.$name] : [Items.$name, Items.$ownerId],
    { unique: change !== "index uniqueness" },
  );
  if (change !== "index removed from definition") Items.addIndex("sync_item_status", [Items.$status]);
  const Links = createTable({
    id: "sync_link",
    schema: z.object({ from: z.string(), to: z.string(), weight: z.number() }),
    primaryKey: change === "primary key" ? ["from", "to", "weight"] : ["from", "to"],
  });
  return { Owners, Items, Links, tables: [Owners, Items, Links] as Table[] };
};

/** A partial index's release: its condition is the change. */
const createPartial = (status: string) => {
  const Tasks = createTable({ id: "sync_task", schema: z.object({ id: id(), status: z.string(), at: z.number() }) });
  Tasks.addIndex("sync_task_open", [Tasks.$at], { where: Tasks.$status.equals(status) });
  return Tasks;
};

type Trip = { method: string; statements: string[] };

/** Records each round trip the adaptor makes: a statement, or statements it sends together. */
const trackTrips = (adaptor: DatabaseAdaptor) => {
  const trips: Trip[] = [];
  let depth = 0;
  const target = adaptor as unknown as Record<string, (...args: any[]) => Promise<unknown>>;
  const originals = new Map<string, (...args: any[]) => Promise<unknown>>();
  for (const method of ["execute", "executeTogether", "executeReads"]) {
    const original = target[method];
    if (typeof original !== "function") continue;
    originals.set(method, original);
    target[method] = async function (this: unknown, ...args: any[]) {
      if (depth === 0) {
        const statements = (method === "execute" ? [args[0]] : args[0]) as { [TO_SQL_SYMBOL](): string }[];
        trips.push({ method, statements: statements.map((statement) => statement[TO_SQL_SYMBOL]()) });
      }
      depth += 1;
      try {
        return await original.apply(this, args);
      } finally {
        depth -= 1;
      }
    };
  }
  const writes = () =>
    trips.flatMap((trip) => trip.statements).filter((statement) => !/^\s*(SELECT|WITH|PRAGMA|SHOW)\b/i.test(statement));
  return {
    trips,
    writes,
    reset: () => {
      trips.length = 0;
    },
    restore: () => {
      for (const [method, original] of originals) target[method] = original;
    },
  };
};

describe.each(TEST_DATABASE_FACTORIES)("schema sync: $name ($connection)", ({ name: engine, create }) => {
  let context: TestDatabaseContext;
  const mysql = engine === "mysql" || engine === "mariadb";
  // MySQL can't send several statements together, so its two schema-wide reads go one by one.
  const readTrips = mysql ? 2 : 1;
  beforeEach(async () => {
    context = await create();
  });
  afterEach(async () => {
    await context.close();
  });
  const adaptorOf = () => (context.db as unknown as { options: { adaptor: DatabaseAdaptor } }).options.adaptor;
  const live = async (tables?: readonly Table[]) => (await adaptorOf().fetchLiveSchema(tables))!;

  it("reads each table alike, alone or with the rest of the schema", async () => {
    const { tables, Items } = createSchema();
    await context.db.syncTables(tables);
    const all = await live();
    const defined = await live(tables);
    for (const table of tables) {
      const name = String(table.id);
      expect(defined.get(name)).toEqual(all.get(name));
      expect((await live([table])).get(name)).toEqual(all.get(name));
      expect((await adaptorOf().fetchTableColumns(table)).results).toEqual(all.get(name)!.columns);
    }
    // What the read gives, column by column.
    const item = all.get("sync_item")!;
    expect(item.columns.map(({ name, notNull, primaryKey }) => ({ name, notNull, primaryKey }))).toEqual([
      { name: "id", notNull: true, primaryKey: true },
      { name: "ownerId", notNull: true, primaryKey: false },
      { name: "name", notNull: true, primaryKey: false },
      { name: "status", notNull: false, primaryKey: false },
      { name: "rank", notNull: false, primaryKey: false },
    ]);
    expect(item.columns.find((column) => column.name === "ownerId")?.foreignKey).toMatchObject({
      table: "sync_owner",
      field: "id",
      onDelete: "cascade",
    });
    expect(
      item.indexes
        .filter((index) => Items.indexes.some((declared) => declared.id === index.name))
        .map(({ name, unique, columns, partial }) => ({ name, unique, columns, partial }))
        .sort((a, b) => a.name.localeCompare(b.name)),
    ).toEqual([
      { name: "sync_item_name", unique: true, columns: ["name", "ownerId"], partial: false },
      { name: "sync_item_status", unique: false, columns: ["status"], partial: false },
    ]);
    expect(
      all
        .get("sync_link")!
        .columns.filter((column) => column.primaryKey)
        .map((column) => column.name),
    ).toEqual(["from", "to"]);
    expect((await live([createTable({ id: "sync_missing", schema: z.object({ id: id() }) })])).size).toBe(0);
  });

  it("reads a current schema in one round trip and writes nothing", async () => {
    const { tables } = createSchema();
    expect((await context.db.syncTables(tables)).map((change) => change.table)).toEqual([
      "sync_owner",
      "sync_item",
      "sync_link",
    ]);
    const tracked = trackTrips(adaptorOf());
    try {
      expect(await context.db.syncTables(tables)).toEqual([]);
      expect(tracked.writes()).toEqual([]);
      expect(tracked.trips).toHaveLength(readTrips);
    } finally {
      tracked.restore();
    }
  });

  it("reads many tables in as many round trips as one", async () => {
    const many = Array.from({ length: 25 }, (_, index) => {
      const table = createTable({ id: `sync_many_${index}`, schema: z.object({ id: id(), value: z.string() }) });
      table.addIndex(`sync_many_${index}_value`, [table.$value]);
      return table;
    });
    await context.db.syncTables(many);
    const tracked = trackTrips(adaptorOf());
    try {
      expect(await context.db.syncTables(many)).toEqual([]);
      expect(tracked.trips).toHaveLength(readTrips);
      expect(tracked.writes()).toEqual([]);
    } finally {
      tracked.restore();
    }
  }, 120_000);

  const changes: { change: string; table: string; expected: Record<string, unknown>; skip?: boolean }[] = [
    { change: "column added", table: "sync_item", expected: { columns: ["colour"] } },
    { change: "column removed", table: "sync_item", expected: { columns: ["rank"] } },
    { change: "nullability", table: "sync_item", expected: { columns: ["name"] } },
    { change: "foreign key", table: "sync_item", expected: { columns: ["ownerId"] } },
    { change: "primary key", table: "sync_link", expected: { primaryKey: true } },
    { change: "index columns", table: "sync_item", expected: { indexes: ["sync_item_name"] } },
    { change: "index uniqueness", table: "sync_item", expected: { indexes: ["sync_item_name"] } },
  ];
  for (const { change, table, expected } of changes) {
    it(`syncs a ${change} in the one table it touches, once`, async () => {
      await context.db.syncTables(createSchema().tables);
      const next = createSchema(change);
      const synced = await context.db.syncTables(next.tables);
      expect(synced.map((entry) => entry.table)).toEqual([table]);
      expect(synced[0]).toMatchObject(expected);
      expect(await context.db.syncTables(next.tables)).toEqual([]);
      const current = (await live(next.tables)).get(table)!;
      const definition = next.tables.find((candidate) => String(candidate.id) === table)!;
      expect(current.columns.map((column) => column.name).sort()).toEqual(Object.keys(definition.fields).sort());
    }, 60_000);
  }

  it("keeps the rows of a table whose index it rebuilds", async () => {
    const { tables, Owners, Items } = createSchema();
    await context.db.syncTables(tables);
    await context.db.insert(Owners, { id: "o", name: "Owner" });
    await context.db.insert(Items, { id: "i", ownerId: "o", name: "Item", status: null, rank: 1 });
    await context.db.syncTables(createSchema("index columns").tables);
    expect((await context.db.select(Items, { id: Items.$id })).results).toEqual([{ id: "i" }]);
  }, 60_000);

  it.skipIf(mysql)(
    "rebuilds a partial index whose condition changed, once",
    async () => {
      await context.db.syncTables([createPartial("open")]);
      const next = createPartial("waiting");
      expect(await context.db.syncTables([next])).toEqual([
        {
          table: "sync_task",
          created: false,
          columns: [],
          primaryKey: false,
          indexes: ["sync_task_open"],
          locality: false,
        },
      ]);
      expect(await context.db.syncTables([next])).toEqual([]);
      const index = (await live([next]))
        .get("sync_task")!
        .indexes.find((candidate) => candidate.name === "sync_task_open");
      expect(index).toMatchObject({ partial: true, columns: ["at"] });
    },
    60_000,
  );

  it("creates an index dropped from an otherwise current table", async () => {
    const { tables, Items } = createSchema();
    await context.db.syncTables(tables);
    await context.db.execute(
      raw(
        mysql
          ? "DROP INDEX `sync_item_status` ON `sync_item`"
          : `DROP INDEX ${engine === "cockroach" ? '"sync_item"@' : ""}"sync_item_status"`,
      ),
    );
    expect(await context.db.syncTables(tables)).toEqual([
      {
        table: "sync_item",
        created: false,
        columns: [],
        primaryKey: false,
        indexes: ["sync_item_status"],
        locality: false,
      },
    ]);
    expect((await live([Items])).get("sync_item")!.indexes.some((index) => index.name === "sync_item_status")).toBe(
      true,
    );
  }, 60_000);

  it("leaves tables, and indexes, outside the definitions alone", async () => {
    const { tables } = createSchema();
    const Other = createTable({ id: "sync_other", schema: z.object({ id: id(), note: z.string() }) });
    Other.addIndex("sync_other_note", [Other.$note]);
    await context.db.syncTables([...tables, Other]);
    const next = createSchema("index removed from definition");
    expect(await context.db.syncTables(next.tables)).toEqual([]);
    const schema = await live();
    expect(schema.get("sync_other")?.indexes.map((index) => index.name)).toContain("sync_other_note");
    // An index the definitions no longer name stays: dropping is a migration's.
    expect(schema.get("sync_item")?.indexes.map((index) => index.name)).toContain("sync_item_status");
  }, 60_000);

  it("syncs names that need quoting, in any case", async () => {
    const Odd = createTable({
      id: 'Sync "Odd" Table',
      schema: z.object({ "Key Column": id(), mixedCase: z.string(), "it's": z.string().nullable() }),
    });
    Odd.addIndex('Sync "Odd" index', [Odd.$mixedCase, Odd["$it's"]]);
    const Lower = createTable({ id: "sync_case", schema: z.object({ id: id(), value: z.string() }) });
    const Upper = createTable({ id: "SYNC_CASE_UPPER", schema: z.object({ id: id(), value: z.string() }) });
    const tables = [Odd, Lower, Upper];
    await context.db.syncTables(tables);
    expect(await context.db.syncTables(tables)).toEqual([]);
    const schema = await live(tables);
    expect([...schema.keys()].sort()).toEqual(["SYNC_CASE_UPPER", 'Sync "Odd" Table', "sync_case"].sort());
    expect(schema.get('Sync "Odd" Table')!.columns.map((column) => column.name)).toEqual([
      "Key Column",
      "mixedCase",
      "it's",
    ]);
    expect(schema.get('Sync "Odd" Table')!.indexes.map((index) => index.name)).toContain('Sync "Odd" index');
  }, 60_000);

  it("creates a table dropped after the schema was read, when it was to change", async () => {
    const { tables } = createSchema();
    await context.db.syncTables(tables);
    const adaptor = adaptorOf();
    const read = adaptor.fetchLiveSchema.bind(adaptor);
    adaptor.fetchLiveSchema = async (requested) => {
      const schema = await read(requested);
      await context.db.execute(raw(mysql ? "DROP TABLE `sync_link`" : 'DROP TABLE "sync_link"'));
      adaptor.fetchLiveSchema = read;
      return schema;
    };
    const next = createSchema("primary key");
    const synced = await context.db.syncTables(next.tables);
    expect(synced.map(({ table, created }) => ({ table, created }))).toEqual([{ table: "sync_link", created: true }]);
    expect(
      (await live(next.tables))
        .get("sync_link")!
        .columns.filter((column) => column.primaryKey)
        .map((column) => column.name),
    ).toEqual(["from", "to", "weight"]);
  }, 60_000);

  // An adaptor that reads tables only one at a time: syncTables syncs each on its own.
  it.skipIf(mysql)(
    "syncs table by table where the adaptor can't read its schema at once",
    async () => {
      const adaptor = adaptorOf();
      const read = adaptor.fetchLiveSchema.bind(adaptor);
      const fetchTableColumns = adaptor.fetchTableColumns;
      // Its own read of one table's columns, as such an adaptor implements it.
      adaptor.fetchTableColumns = async (table) => {
        const columns = (await read([table]))!.get(String(table.id))?.columns ?? [];
        return { results: columns, first: columns[0] };
      };
      adaptor.fetchLiveSchema = async () => undefined;
      try {
        const created = await context.db.syncTables(createSchema().tables);
        expect(created.map(({ table, created }) => ({ table, created }))).toEqual([
          { table: "sync_owner", created: true },
          { table: "sync_item", created: true },
          { table: "sync_link", created: true },
        ]);
        const added = await context.db.syncTables(createSchema("column added").tables);
        expect(added.map(({ table, columns }) => ({ table, columns }))).toEqual([
          { table: "sync_item", columns: ["colour"] },
        ]);
        expect(await context.db.syncTables(createSchema("column added").tables)).toEqual([]);
      } finally {
        adaptor.fetchLiveSchema = read;
        adaptor.fetchTableColumns = fetchTableColumns;
      }
      const item = (await live()).get("sync_item")!;
      expect(item.columns.map((column) => column.name)).toContain("colour");
      expect(item.indexes.map((index) => index.name)).toEqual(
        expect.arrayContaining(["sync_item_name", "sync_item_status"]),
      );
    },
    60_000,
  );

  it("returns what syncing one table changed", async () => {
    const { Owners } = createSchema();
    expect(await context.db.syncTable(Owners)).toEqual({
      table: "sync_owner",
      created: true,
      columns: [],
      primaryKey: false,
      indexes: [],
      locality: false,
    });
    expect(await context.db.syncTable(Owners)).toBeUndefined();
  });

  it.skipIf(engine !== "cockroach")(
    "sets a table's locality only when it differs, from the one read",
    async () => {
      const { first } = await context.db.execute(sql`SELECT current_database() AS name`);
      await context.db.execute(sql`ALTER DATABASE ${raw(`"${first!.name}"`)} PRIMARY REGION "aws-ap-southeast-1"`);
      const regional = (type: "global" | "regional-by-row") =>
        createTable({
          id: "sync_regional",
          schema: z
            .object({ id: id(), homeRegion: z.string().meta(metaStore([cockroachRegion()])) })
            .meta(metaStore([cockroachLocality(type === "global" ? { type } : { type, regionColumn: "homeRegion" })])),
        });
      await context.db.syncTables([regional("regional-by-row")]);
      const tracked = trackTrips(adaptorOf());
      try {
        expect(await context.db.syncTables([regional("regional-by-row")])).toEqual([]);
        expect(tracked.trips).toHaveLength(1);
        tracked.reset();
        expect(await context.db.syncTables([regional("global")])).toEqual([
          { table: "sync_regional", created: false, columns: [], primaryKey: false, indexes: [], locality: true },
        ]);
        expect(tracked.writes()).toEqual(['ALTER TABLE "sync_regional" SET LOCALITY GLOBAL']);
      } finally {
        tracked.restore();
      }
      expect((await live()).get("sync_regional")?.locality).toBe("GLOBAL");
      expect(await context.db.syncTables([regional("global")])).toEqual([]);
    },
    120_000,
  );
});
