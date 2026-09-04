import * as zod from "zod";
import { createTable, Database, foreignKey, metaStore, primaryKey } from "../src";
import type DatabaseAdaptor from "../src/DatabaseAdaptor";
import type { SelectQuery, SqlResult } from "../src/QueryBuilder";
import type { Statement } from "../src/Statement";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const createPeopleTable = () =>
  createTable({
    id: "contract_people",
    schema: zod.object({
      id: zod.string().meta(metaStore([primaryKey()])),
      name: zod.string(),
      age: zod.number(),
      nickname: zod.string().nullable(),
    }),
  });

const people = [
  { id: "1", name: "Ada", age: 36, nickname: null },
  { id: "2", name: "Grace", age: 45, nickname: "Amazing Grace" },
  { id: "3", name: "Linus", age: 28, nickname: null },
  { id: "4", name: "Margaret", age: 45, nickname: "Maggie" },
];

const withMetadata = <TSchema extends zod.ZodType>(schema: TSchema, ...metadata: Parameters<TSchema["meta"]>[0][]) =>
  schema.meta(metaStore(metadata));

describe.each(TEST_DATABASE_FACTORIES)("database contract: $name", ({ create }) => {
  let context: TestDatabaseContext;
  let db: Database;
  let PeopleTable: ReturnType<typeof createPeopleTable>;

  beforeEach(async () => {
    context = await create();
    db = context.db;
    PeopleTable = createPeopleTable();
    await db.syncTable(PeopleTable);
  });

  afterEach(async () => {
    await context.close();
  });

  it("inserts and selects projected fields", async () => {
    await db.insertMany(PeopleTable, people);

    const result = await db.select(PeopleTable, ["id", "name"]).orderBy(PeopleTable.$id, "ASC");

    expect(result.results).toEqual(people.map(({ id, name }) => ({ id, name })));
    expect(result.first).toEqual({ id: "1", name: "Ada" });
  });

  it("supports every comparison and compound condition", async () => {
    await db.insertMany(PeopleTable, people);

    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$name.like("M%"))).results).toEqual([{ id: "4" }]);
    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$age.greaterThan(36))).results).toHaveLength(2);
    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$age.greaterThanOrEquals(36))).results).toHaveLength(
      3,
    );
    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$age.lessThan(36))).results).toEqual([{ id: "3" }]);
    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$age.lessThanOrEquals(36))).results).toHaveLength(2);
    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$name.notEquals("Ada"))).results).toHaveLength(3);
    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$id.in(["1", "3"]))).results).toEqual([
      { id: "1" },
      { id: "3" },
    ]);
    expect(
      (
        await db
          .select(PeopleTable, ["id"])
          .where(PeopleTable.$age.equals(45).and(PeopleTable.$nickname.notEquals(null)))
      ).results,
    ).toEqual([{ id: "2" }, { id: "4" }]);
    expect(
      (
        await db
          .select(PeopleTable, ["id"])
          .where(PeopleTable.$name.equals("Ada").or(PeopleTable.$name.equals("Linus")))
      ).results,
    ).toEqual([{ id: "1" }, { id: "3" }]);
  });

  it("handles null comparisons", async () => {
    await db.insertMany(PeopleTable, people);

    const nullRows = await db.select(PeopleTable, ["id"]).where(PeopleTable.$nickname.equals(null));
    const nonNullRows = await db.select(PeopleTable, ["id"]).where(PeopleTable.$nickname.notEquals(null));

    expect(nullRows.results).toEqual([{ id: "1" }, { id: "3" }]);
    expect(nonNullRows.results).toEqual([{ id: "2" }, { id: "4" }]);
  });

  it("supports ordering, limit, offset, and one", async () => {
    await db.insertMany(PeopleTable, people);

    const page = await db
      .select(PeopleTable, ["id"])
      .orderBy(PeopleTable.$age, "DESC")
      .orderBy(PeopleTable.$id, "ASC")
      .limit(2)
      .offset(1);
    const one = await db.select(PeopleTable, ["id"]).orderBy(PeopleTable.$id, "DESC").one();

    expect(page.results).toEqual([{ id: "4" }, { id: "1" }]);
    expect(page.limit).toBeUndefined();
    expect(one.results).toEqual([{ id: "4" }]);
  });

  it("treats limit zero and an empty IN list as empty results", async () => {
    await db.insertMany(PeopleTable, people);

    expect((await db.select(PeopleTable, ["id"]).limit(0)).results).toEqual([]);
    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$id.in([]))).results).toEqual([]);
  });

  it("supports repeated where calls and ignores conditional falsy clauses", async () => {
    await db.insertMany(PeopleTable, people);

    const result = await db
      .select(PeopleTable, ["id"])
      .where(PeopleTable.$age.greaterThanOrEquals(36))
      .where(PeopleTable.$name.notEquals("Grace").and(false, undefined, null, "", 0));

    expect(result.results).toEqual([{ id: "1" }, { id: "4" }]);
  });

  it("changes projections with fields without mutating the source builder", async () => {
    await db.insertMany(PeopleTable, people);

    const base = db.select(PeopleTable, ["id"]);
    const names = base.clone().fields("name");

    expect((await base.limit(1)).first).toEqual({ id: "1" });
    expect((await names.limit(1)).first).toEqual({ name: "Ada" });
  });

  it("keeps cloned builders independent", async () => {
    await db.insertMany(PeopleTable, people);

    const base = db.select(PeopleTable, ["id"]);
    const descending = base.clone().orderBy(PeopleTable.$id, "DESC").limit(1);
    const ascending = base.clone().orderBy(PeopleTable.$id, "ASC").limit(1);

    expect((await descending).first).toEqual({ id: "4" });
    expect((await ascending).first).toEqual({ id: "1" });
    expect((await base).results).toEqual([{ id: "1" }, { id: "2" }, { id: "3" }, { id: "4" }]);
  });

  it("counts all rows and selected fields with filters", async () => {
    await db.insertMany(PeopleTable, people);

    expect((await db.count(PeopleTable)).first).toEqual({ _count: 4 });
    expect((await db.count(PeopleTable, "nickname")).first).toEqual({ nickname: 2 });
    expect((await db.count(PeopleTable).where(PeopleTable.$age.equals(45))).first).toEqual({ _count: 2 });
    expect((await db.select(PeopleTable, ["id"]).where(PeopleTable.$age.equals(45)).count()).first).toEqual({ id: 2 });
  });

  it("supports insert, update, updateMany, upsert, and delete", async () => {
    await db.insert(PeopleTable, people[0]);
    await db.insertMany(PeopleTable, people.slice(1));
    await db.update(PeopleTable, { age: 37 }, PeopleTable.$id.equals("1"));
    await db.updateMany(
      PeopleTable,
      [
        { id: "2", age: 46 },
        { id: "3", age: 29 },
      ],
      PeopleTable.$id,
    );
    await db.upsert(PeopleTable, { id: "4", name: "Margaret Hamilton", age: 46, nickname: "Maggie" }, PeopleTable.$id);
    await db.delete(PeopleTable).where(PeopleTable.$id.equals("3"));

    const result = await db.select(PeopleTable, ["id", "name", "age"]).orderBy(PeopleTable.$id, "ASC");
    expect(result.results).toEqual([
      { id: "1", name: "Ada", age: 37 },
      { id: "2", name: "Grace", age: 46 },
      { id: "4", name: "Margaret Hamilton", age: 46 },
    ]);
  });

  it("returns parsed and mutated rows", async () => {
    const parsed = await db.insert(PeopleTable, people[0]).selectParsed();
    const inserted = await db.insert(PeopleTable, people[1]).selectMutated();
    const updated = await db.update(PeopleTable, { age: 37 }, PeopleTable.$id.equals("1")).selectMutated("id", "age");

    expect(parsed.first).toEqual(people[0]);
    expect(inserted.first).toEqual(people[1]);
    expect(updated.results).toEqual([{ id: "1", age: 37 }]);
  });

  it("treats empty bulk operations as no-ops", async () => {
    expect(await db.insertMany(PeopleTable, [])).toEqual({ results: [], first: undefined });
    expect(await db.updateMany(PeopleTable, [], PeopleTable.$id)).toMatchObject({ results: [], first: undefined });
    expect(await db.updateMany(PeopleTable, [], PeopleTable.$id).selectMutated()).toEqual({
      results: [],
      first: undefined,
    });
  });

  it("aligns optional fields across heterogeneous bulk inserts", async () => {
    const OptionalTable = createTable({
      id: "contract_optional_people",
      schema: zod.object({
        id: zod.string().meta(metaStore([primaryKey()])),
        name: zod.string(),
        nickname: zod.string().optional(),
      }),
    });
    await db.syncTable(OptionalTable);

    await db.insertMany(OptionalTable, [
      { id: "1", name: "Ada" },
      { id: "2", name: "Grace", nickname: "Amazing Grace" },
      { id: "3", name: "Linus" },
    ]);

    expect((await db.select(OptionalTable, ["*"]).orderBy(OptionalTable.$id, "ASC")).results).toEqual([
      { id: "1", name: "Ada", nickname: null },
      { id: "2", name: "Grace", nickname: "Amazing Grace" },
      { id: "3", name: "Linus", nickname: null },
    ]);
  });

  it("commits and rolls back transactions", async () => {
    await db.transaction(async (transaction) => {
      await transaction.insert(PeopleTable, people[0]);
      expect((await transaction.select(PeopleTable, ["id"])).results).toEqual([{ id: "1" }]);
      await transaction.update(PeopleTable, { age: 37 }, PeopleTable.$id.equals("1"));
    });
    expect((await db.select(PeopleTable, ["age"])).results).toEqual([{ age: 37 }]);

    await expect(
      db.transaction(async (transaction) => {
        await transaction.insert(PeopleTable, people[1]);
        throw new Error("rollback");
      }),
    ).rejects.toThrow("rollback");
    expect((await db.select(PeopleTable, ["id"]).orderBy(PeopleTable.$id, "ASC")).results).toEqual([{ id: "1" }]);

    await expect(db.transaction(async (transaction) => transaction.transaction(async () => undefined))).rejects.toThrow(
      "Nested transactions are not supported",
    );
  });

  it("returns callback values and preserves thrown values", async () => {
    const result = await db.transaction(async (transaction) => {
      await transaction.insert(PeopleTable, people[0]);
      return { id: "1", committed: true } as const;
    });

    expect(result).toEqual({ id: "1", committed: true });

    const rejection = { reason: "cancelled" };
    await expect(
      db.transaction(async (transaction) => {
        await transaction.insert(PeopleTable, people[1]);
        throw rejection;
      }),
    ).rejects.toBe(rejection);
    expect((await db.select(PeopleTable, ["id"]).orderBy(PeopleTable.$id, "ASC")).results).toEqual([{ id: "1" }]);
  });

  it("rolls back database errors and remains usable afterwards", async () => {
    await expect(
      db.transaction(async (transaction) => {
        await transaction.insert(PeopleTable, people[0]);
        await transaction.insert(PeopleTable, people[0]);
      }),
    ).rejects.toBeDefined();

    expect((await db.select(PeopleTable, ["id"])).results).toEqual([]);
    await db.transaction(async (transaction) => transaction.insert(PeopleTable, people[1]));
    expect((await db.select(PeopleTable, ["id"])).results).toEqual([{ id: "2" }]);
  });

  it("supports concurrent independent top-level transactions", async () => {
    await Promise.all(
      people.map((person) => db.transaction(async (transaction) => transaction.insert(PeopleTable, person))),
    );

    expect((await db.select(PeopleTable, ["id"]).orderBy(PeopleTable.$id, "ASC")).results).toEqual([
      { id: "1" },
      { id: "2" },
      { id: "3" },
      { id: "4" },
    ]);
  });

  it("enforces foreign keys and cascades through multiple levels", async () => {
    const ParentTable = createTable({
      id: "contract_parents",
      schema: zod.object({ id: withMetadata(zod.string(), primaryKey()) }),
    });
    const ChildTable = createTable({
      id: "contract_children",
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        parentId: withMetadata(zod.string(), foreignKey({ field: ParentTable.$id, onDelete: "cascade" })),
      }),
    });
    const GrandchildTable = createTable({
      id: "contract_grandchildren",
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        childId: withMetadata(zod.string(), foreignKey({ field: ChildTable.$id, onDelete: "cascade" })),
      }),
    });
    await db.syncTable(ParentTable);
    await db.syncTable(ChildTable);
    await db.syncTable(GrandchildTable);

    await expect(Promise.resolve(db.insert(ChildTable, { id: "orphan", parentId: "missing" }))).rejects.toBeDefined();
    await db.insert(ParentTable, { id: "parent" });
    await db.insert(ChildTable, { id: "child", parentId: "parent" });
    await db.insert(GrandchildTable, { id: "grandchild", childId: "child" });
    await db.delete(ParentTable).where(ParentTable.$id.equals("parent"));

    expect((await db.select(ChildTable, ["*"])).results).toEqual([]);
    expect((await db.select(GrandchildTable, ["*"])).results).toEqual([]);
  });

  it("supports restrictive and set-null foreign-key actions", async () => {
    const ParentTable = createTable({
      id: "contract_action_parents",
      schema: zod.object({ id: withMetadata(zod.string(), primaryKey()) }),
    });
    const RestrictedChildTable = createTable({
      id: "contract_restricted_children",
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        parentId: withMetadata(zod.string(), foreignKey({ field: ParentTable.$id, onDelete: "restrict" })),
      }),
    });
    const NullableChildTable = createTable({
      id: "contract_nullable_children",
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        parentId: withMetadata(zod.string().nullable(), foreignKey({ field: ParentTable.$id, onDelete: "set null" })),
      }),
    });
    await db.syncTable(ParentTable);
    await db.syncTable(RestrictedChildTable);
    await db.syncTable(NullableChildTable);
    await db.insert(ParentTable, { id: "restricted" });
    await db.insert(ParentTable, { id: "nullable" });
    await db.insert(RestrictedChildTable, { id: "restricted-child", parentId: "restricted" });
    await db.insert(NullableChildTable, { id: "nullable-child", parentId: "nullable" });

    await expect(
      Promise.resolve(db.delete(ParentTable).where(ParentTable.$id.equals("restricted"))),
    ).rejects.toBeDefined();
    expect((await db.select(ParentTable, ["id"]).where(ParentTable.$id.equals("restricted"))).first).toEqual({
      id: "restricted",
    });

    await db.delete(ParentTable).where(ParentTable.$id.equals("nullable"));
    expect((await db.select(NullableChildTable, ["parentId"])).first).toEqual({ parentId: null });
  });

  it("rolls back cascaded deletes with the surrounding transaction", async () => {
    const ParentTable = createTable({
      id: "contract_rollback_parents",
      schema: zod.object({ id: withMetadata(zod.string(), primaryKey()) }),
    });
    const ChildTable = createTable({
      id: "contract_rollback_children",
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        parentId: withMetadata(zod.string(), foreignKey({ field: ParentTable.$id, onDelete: "cascade" })),
      }),
    });
    await db.syncTable(ParentTable);
    await db.syncTable(ChildTable);
    await db.insert(ParentTable, { id: "parent" });
    await db.insert(ChildTable, { id: "child", parentId: "parent" });

    await expect(
      db.transaction(async (transaction) => {
        await transaction.delete(ParentTable).where(ParentTable.$id.equals("parent"));
        expect((await transaction.select(ChildTable, ["id"])).results).toEqual([]);
        throw new Error("rollback cascade");
      }),
    ).rejects.toThrow("rollback cascade");

    expect((await db.select(ParentTable, ["id"])).results).toEqual([{ id: "parent" }]);
    expect((await db.select(ChildTable, ["id"])).results).toEqual([{ id: "child" }]);
  });

  it("migrates, changes, and removes foreign keys without losing rows", async () => {
    const ParentTable = createTable({
      id: "contract_migration_parents",
      schema: zod.object({ id: withMetadata(zod.string(), primaryKey()) }),
    });
    const InitialChildTable = createTable({
      id: "contract_migration_children",
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        parentId: zod.string().nullable(),
      }),
    });
    await db.syncTable(ParentTable);
    await db.syncTable(InitialChildTable);
    await db.insert(ParentTable, { id: "parent" });
    await db.insert(InitialChildTable, { id: "child", parentId: "parent" });

    const CascadingChildTable = createTable({
      id: InitialChildTable.id,
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        parentId: withMetadata(zod.string().nullable(), foreignKey({ field: ParentTable.$id, onDelete: "cascade" })),
      }),
    });
    await db.syncTable(CascadingChildTable);
    await db.syncTable(CascadingChildTable);
    expect((await db.select(CascadingChildTable, ["*"])).results).toEqual([{ id: "child", parentId: "parent" }]);

    const SetNullChildTable = createTable({
      id: InitialChildTable.id,
      schema: zod.object({
        id: withMetadata(zod.string(), primaryKey()),
        parentId: withMetadata(zod.string().nullable(), foreignKey({ field: ParentTable.$id, onDelete: "set null" })),
      }),
    });
    await db.syncTable(SetNullChildTable);
    await db.delete(ParentTable).where(ParentTable.$id.equals("parent"));
    expect((await db.select(SetNullChildTable, ["parentId"])).first).toEqual({ parentId: null });

    await db.syncTable(InitialChildTable);
    await db.insert(InitialChildTable, { id: "orphan", parentId: "missing" });
    expect((await db.select(InitialChildTable, ["id"]).orderBy(InitialChildTable.$id, "ASC")).results).toEqual([
      { id: "child" },
      { id: "orphan" },
    ]);
  });

  it("rejects unsafe or invalid mutations", async () => {
    expect(() => db.update(PeopleTable, {}, PeopleTable.$id.equals("1"))).toThrow("No values to update");
    expect(() => db.insert(PeopleTable, { id: "1", name: "Ada", age: "old", nickname: null } as never)).toThrow();
    await expect(Promise.resolve(db.delete(PeopleTable))).rejects.toThrow(
      "Delete without where condition is not allowed",
    );
  });
});

class CountingAdaptor implements Partial<DatabaseAdaptor> {
  executions = 0;

  async executeSelect(_query: SelectQuery): Promise<SqlResult> {
    this.executions += 1;
    return { results: [], first: undefined };
  }

  async execute(_statement: Statement): Promise<SqlResult> {
    throw new Error("Not used");
  }
}

it("executes a lazy query only once across then, catch, and finally", async () => {
  const adaptor = new CountingAdaptor();
  const db = new Database({ adaptor: adaptor as DatabaseAdaptor });
  const PeopleTable = createPeopleTable();
  const query = db.select(PeopleTable, ["id"]);

  await Promise.all([query.then(), query.finally(() => undefined), query.catch(() => undefined)]);

  expect(adaptor.executions).toBe(1);
});
