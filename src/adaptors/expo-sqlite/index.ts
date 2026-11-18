import type * as zod from "zod";
import { keyCondition } from "../../DatabaseAdaptor";
import type {
  InputOfTable,
  SelectCondition,
  SingleFieldBinding,
  SqlResult,
  StringKeys,
  ValueOfTable,
} from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import type { Table } from "../../Table";
import SqliteAdaptor from "../sqlite";

// Type for expo-sqlite database
export interface ExpoSQLiteDatabase {
  execAsync(query: string, params?: any): Promise<any>;
  runAsync(query: string, ...params: any[]): Promise<any>;
  getAllAsync(query: string, ...params: any[]): Promise<any[]>;
  getFirstAsync(query: string, ...params: any[]): Promise<any>;
}

export default class ExpoSQLiteAdaptor extends SqliteAdaptor<ExpoSQLiteDatabase> {
  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const rawSql = this.sqlText(statement);

    try {
      // Use getAllAsync for SELECT queries, runAsync for others
      const normalizedSql = rawSql.trim().toUpperCase();
      const returnsRows =
        normalizedSql.startsWith("SELECT") ||
        normalizedSql.startsWith("PRAGMA") ||
        normalizedSql.startsWith("WITH") ||
        normalizedSql.startsWith("EXPLAIN") ||
        /\bRETURNING\b/.test(normalizedSql);

      let results: any[];

      if (returnsRows) {
        results = await this.driver.getAllAsync(rawSql);
      } else {
        // For INSERT, UPDATE, DELETE, etc.
        await this.driver.runAsync(rawSql);
        results = [];
      }

      return this.mapResult({
        results: results,
        first: results[0],
      });
    } catch (error) {
      console.error("ExpoSQLiteAdaptor.execute error:", error);
      console.error("SQL:", rawSql);
      throw error;
    }
  }

  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(
    table: TTable,
    values: TValue[],
    fields: readonly SingleFieldBinding<TValue, TKey>[],
    where?: SelectCondition,
  ): Promise<SqlResult<void, 0>> {
    // Execute updates sequentially for expo-sqlite
    // (expo-sqlite doesn't have batch support like D1)
    for (const value of values) {
      const sql = this.sqlText(this.buildUpdateSql(table, value, keyCondition(fields, value, where) as any));
      await this.driver.runAsync(sql);
    }

    return {
      results: [],
      first: undefined,
    };
  }
}
