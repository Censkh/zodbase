import * as zod from "zod";
import { createTable } from "../src";
import {
  acquireTestDatabaseContainers,
  releaseTestDatabaseContainers,
  TEST_DATABASE_FACTORIES,
  type TestDatabaseContext,
} from "./helpers/databaseContract";

beforeAll(acquireTestDatabaseContainers, 180_000);
afterAll(releaseTestDatabaseContainers, 180_000);

describe.each(TEST_DATABASE_FACTORIES)("identifier security: $name", ({ create }) => {
  let context: TestDatabaseContext;

  beforeEach(async () => {
    context = await create();
  });

  afterEach(async () => {
    await context.close();
  });

  it("quotes table, column, and index identifiers", async () => {
    const SafeTable = createTable({
      id: "identifier_safe",
      schema: zod.object({ id: zod.string() }),
    });
    await context.db.syncTable(SafeTable);
    await context.db.insert(SafeTable, { id: "still-here" });

    const oddTableName = 'odd"; DROP TABLE identifier_safe; --';
    const oddColumnName = 'display"name';
    const oddIndexName = 'odd-index"; DROP TABLE identifier_safe; --';
    const OddTable = createTable({
      id: oddTableName,
      schema: zod.object({
        id: zod.string(),
        [oddColumnName]: zod.string(),
      }),
    });
    const oddColumn = OddTable.fields[oddColumnName as keyof typeof OddTable.fields];
    OddTable.addIndex(oddIndexName, [oddColumn]);

    await context.db.syncTable(OddTable);
    await context.db.insert(OddTable, { id: "1", [oddColumnName]: "quoted value" });

    expect((await context.db.select(OddTable, ["*"]).where(oddColumn.equals("quoted value"))).first).toEqual({
      id: "1",
      [oddColumnName]: "quoted value",
    });
    expect((await context.db.select(SafeTable, ["*"])).first).toEqual({ id: "still-here" });
  });
});
