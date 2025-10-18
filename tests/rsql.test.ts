import BunDatabase from "bun:sqlite";
import * as zod from "zod/v4";
import { createTable, Database } from "../src";
import BunSqliteAdaptor from "../src/adaptors/bun-sqlite";

// Test table schema
const AssetTable = createTable({
  id: "asset",
  schema: zod.object({
    id: zod.string(),
    name: zod.string(),
    status: zod.string(),
    price: zod.number(),
    quantity: zod.number(),
    isActive: zod.boolean(),
    category: zod.string(),
    parentId: zod.string().nullable(),
  }),
});

// Test data
const testAssets = [
  {
    id: "1",
    name: "Asset One",
    status: "approved",
    price: 100,
    quantity: 10,
    isActive: true,
    category: "electronics",
    parentId: null,
  },
  {
    id: "2",
    name: "Asset Two",
    status: "rejected",
    price: 200,
    quantity: 5,
    isActive: false,
    category: "electronics",
    parentId: null,
  },
  {
    id: "3",
    name: "Asset Three",
    status: "pending",
    price: 150,
    quantity: 20,
    isActive: true,
    category: "furniture",
    parentId: "1",
  },
  {
    id: "4",
    name: "Asset Four",
    status: "rejected",
    price: 50,
    quantity: 15,
    isActive: false,
    category: "furniture",
    parentId: null,
  },
  {
    id: "5",
    name: "Asset Five",
    status: "approved",
    price: 300,
    quantity: 8,
    isActive: true,
    category: "electronics",
    parentId: null,
  },
];

// Helper to create a test database
const createTestDb = async () => {
  const rawDb = new BunDatabase(":memory:");
  const db = new Database({
    adaptor: new BunSqliteAdaptor({
      driver: rawDb,
    }),
  });

  await db.syncTable(AssetTable);
  await db.insertMany(AssetTable, testAssets);

  return db;
};

describe("RSQL Filter", () => {
  describe("Basic Comparison Operators", () => {
    it("should filter with equals operator (==)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status==approved");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(2);
      expect(results.every((r) => r.status === "approved")).toBe(true);
    });

    it("should filter with not equals operator (!=)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status!=approved");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.status !== "approved")).toBe(true);
    });

    it("should filter with greater than operator (>)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "price>150");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(2);
      expect(results.every((r) => r.price > 150)).toBe(true);
    });

    it("should filter with greater than or equals operator (>=)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "price>=150");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.price >= 150)).toBe(true);
    });

    it("should filter with less than operator (<)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "price<150");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(2);
      expect(results.every((r) => r.price < 150)).toBe(true);
    });

    it("should filter with less than or equals operator (<=)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "price<=150");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.price <= 150)).toBe(true);
    });
  });

  describe("IN Operator", () => {
    it("should filter with in operator (=in=)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status=in=(approved,pending)");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.status === "approved" || r.status === "pending")).toBe(true);
    });

    it("should filter with in operator with single value", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status=in=(approved)");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(2);
      expect(results.every((r) => r.status === "approved")).toBe(true);
    });
  });

  describe("Logical Operators", () => {
    it("should filter with AND operator (;)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status==rejected;price>100");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(1);
      expect(results[0].status).toBe("rejected");
      expect(results[0].price).toBe(200);
    });

    it("should filter with OR operator (,)", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status==approved,status==pending");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.status === "approved" || r.status === "pending")).toBe(true);
    });

    it("should filter with complex AND/OR combination", async () => {
      const db = await createTestDb();
      // (status==approved OR status==pending) AND price>100
      const condition = applyRsqlFilter(AssetTable, "(status==approved,status==pending);price>100");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(2);
      expect(results.every((r) => (r.status === "approved" || r.status === "pending") && r.price > 100)).toBe(true);
    });

    it("should filter with multiple AND conditions", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status==approved;price>100;category==electronics");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(1);
      expect(results[0].id).toBe("5");
    });
  });

  describe("Type Coercion", () => {
    it("should parse numeric values correctly", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "quantity==10");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(1);
      expect(results[0].quantity).toBe(10);
    });

    it("should parse boolean values correctly", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "isActive==true");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      // SQLite stores booleans as 1/0
      expect(results.every((r) => r.isActive === true || r.isActive === 1)).toBe(true);
    });

    it("should parse null values correctly", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "parentId==null");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(4);
      expect(results.every((r) => r.parentId === null)).toBe(true);
    });

    it("should parse string values correctly", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "category==electronics");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.category === "electronics")).toBe(true);
    });
  });

  describe("Edge Cases", () => {
    it("should return undefined for empty filter string", () => {
      const condition = applyRsqlFilter(AssetTable, "");
      expect(condition).toBeUndefined();
    });

    it("should return undefined for undefined filter", () => {
      const condition = applyRsqlFilter(AssetTable, undefined);
      expect(condition).toBeUndefined();
    });

    it("should throw error for invalid field name", () => {
      expect(() => {
        applyRsqlFilter(AssetTable, "nonExistentField==value");
      }).toThrow('Field "nonExistentField" not found in table');
    });

    it("should throw error for invalid filter syntax", () => {
      expect(() => {
        applyRsqlFilter(AssetTable, "invalid filter syntax");
      }).toThrow("Failed to parse RSQL filter");
    });
  });

  describe("Real-world Use Cases", () => {
    it("should filter assets by status and exclude sub-assets", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status==rejected;parentId==null");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(2);
      expect(results.every((r) => r.status === "rejected" && r.parentId === null)).toBe(true);
    });

    it("should filter assets by price range", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "price>=100;price<=200");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.price >= 100 && r.price <= 200)).toBe(true);
    });

    it("should filter active assets in multiple categories", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "isActive==true;category=in=(electronics,furniture)");
      const { results } = await db.select(AssetTable, ["*"]).where(condition!);

      expect(results.length).toBe(3);
      expect(
        results.every(
          (r) =>
            // SQLite stores booleans as 1/0
            (r.isActive === true || r.isActive === 1) && (r.category === "electronics" || r.category === "furniture"),
        ),
      ).toBe(true);
    });

    it("should combine with additional where clauses", async () => {
      const db = await createTestDb();
      const condition = applyRsqlFilter(AssetTable, "status==approved");
      const { results } = await db
        .select(AssetTable, ["*"])
        .where(AssetTable.$category.equals("electronics"))
        .where(condition!);

      expect(results.length).toBe(2);
      expect(results.every((r) => r.status === "approved" && r.category === "electronics")).toBe(true);
    });
  });
});
