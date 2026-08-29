import * as zod from "zod/v4";
import { createTable, metaStore, primaryKey } from "../src";
import { rsqlToCondition } from "../src/rsql";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

const createAssetsTable = () =>
  createTable({
    id: "rsql_contract_assets",
    schema: zod.object({
      id: zod.string().meta(metaStore([primaryKey()])),
      code: zod.string(),
      name: zod.string(),
      quantity: zod.number(),
      active: zod.boolean(),
      parentId: zod.string().nullable(),
      tags: zod.array(zod.string()),
    }),
  });

const assets = [
  {
    id: "1",
    code: "00123",
    name: "Ada, Lovelace",
    quantity: 10,
    active: true,
    parentId: null,
    tags: ["keep", "urgent"],
  },
  {
    id: "2",
    code: "42",
    name: "Grace Hopper",
    quantity: 20,
    active: false,
    parentId: "1",
    tags: ["archive"],
  },
  {
    id: "3",
    code: "3:15",
    name: "Flynn's Device",
    quantity: 30,
    active: true,
    parentId: null,
    tags: ["keep"],
  },
];

describe.each(TEST_DATABASE_FACTORIES)("RSQL contract: $name", ({ create }) => {
  let context: TestDatabaseContext;
  let AssetsTable: ReturnType<typeof createAssetsTable>;

  beforeEach(async () => {
    context = await create();
    AssetsTable = createAssetsTable();
    await context.db.syncTable(AssetsTable);
    await context.db.insertMany(AssetsTable, assets);
  });

  afterEach(async () => {
    await context.close();
  });

  const idsFor = async (filter: string) => {
    const condition = rsqlToCondition(AssetsTable, filter);
    const result = await context.db.select(AssetsTable, ["id"]).where(condition!).orderBy(AssetsTable.$id, "ASC");
    return result.results.map(({ id }) => id);
  };

  it("coerces values according to the selected field schema", async () => {
    expect(await idsFor("code==00123")).toEqual(["1"]);
    expect(await idsFor("quantity>=20")).toEqual(["2", "3"]);
    expect(await idsFor("active==true")).toEqual(["1", "3"]);
    expect(await idsFor("parentId==null")).toEqual(["1", "3"]);
  });

  it("supports IN, OUT, and LIKE operators", async () => {
    expect(await idsFor("code=in=(00123,42)")).toEqual(["1", "2"]);
    expect(await idsFor("code=out=(00123,42)")).toEqual(["3"]);
    expect(await idsFor("name=like=Grace%")).toEqual(["2"]);
  });

  it("supports exact membership checks for JSON arrays", async () => {
    expect(await idsFor("tags=in=(urgent)")).toEqual(["1"]);
    expect(await idsFor("tags=in=(keep,urgent)")).toEqual(["1", "3"]);
  });

  it("handles quoted, escaped, and punctuation-heavy values", async () => {
    expect(await idsFor('name=="Ada, Lovelace"')).toEqual(["1"]);
    expect(await idsFor("name=='Flynn\\'s Device'")).toEqual(["3"]);
    expect(await idsFor("code==3:15")).toEqual(["3"]);
  });

  it("honors logical precedence, parentheses, and verbose operators", async () => {
    expect(await idsFor("active==true,quantity==20;parentId!=null")).toEqual(["1", "2", "3"]);
    expect(await idsFor("(active==true,quantity==20);parentId==null")).toEqual(["1", "3"]);
    expect(await idsFor("active==true and quantity>=20")).toEqual(["3"]);
    expect(await idsFor("active==false or quantity==10")).toEqual(["1", "2"]);
  });

  it("rejects unsupported operators and malformed filters", () => {
    expect(() => rsqlToCondition(AssetsTable, "quantity=all=(10,20)")).toThrow("Unsupported operator");
    expect(() => rsqlToCondition(AssetsTable, "quantity=20")).toThrow("Failed to parse RSQL filter");
    expect(() => rsqlToCondition(AssetsTable, "missing==value")).toThrow('Field "missing" not found in table');
  });

  it("keeps SQL-injection payloads as values", async () => {
    const payload = "'; DROP TABLE rsql_contract_assets; --";
    await context.db.insert(AssetsTable, {
      id: "4",
      code: "safe",
      name: payload,
      quantity: 0,
      active: false,
      parentId: null,
      tags: [],
    });

    expect(await idsFor(`name=="${payload}"`)).toEqual(["4"]);
    expect((await context.db.count(AssetsTable)).first?._count).toBe(4);
  });
});
