import type * as zod from "zod/v4";
import type { InputOfTable, SingleFieldBinding, SqlResult, StringKeys, ValueOfTable } from "../../QueryBuilder";
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

    const startTimestamp = Date.now();
    const rawSql = statement[TO_SQL_SYMBOL]();

    try {
      // Use getAllAsync for SELECT queries, runAsync for others
      const isSelect = rawSql.trim().toUpperCase().startsWith("SELECT");
      const isCount = rawSql.trim().toUpperCase().includes("COUNT(");

      let results: any[];

      if (isSelect || isCount) {
        results = await this.driver.getAllAsync(rawSql);
      } else {
        // For INSERT, UPDATE, DELETE, etc.
        await this.driver.runAsync(rawSql);
        results = [];
      }

      if (this.options.debug) {
        console.log("ExpoSQLiteAdaptor.execute", "Executed SQL", {
          sql: rawSql,
          timings: {
            totalDurationMs: Date.now() - startTimestamp,
          },
        });
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
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>> {
    // Execute updates sequentially for expo-sqlite
    // (expo-sqlite doesn't have batch support like D1)
    for (const value of values) {
      const sql = this.buildUpdateSql(table, value, field.equals(value[field.key] as any) as any)[TO_SQL_SYMBOL]();
      await this.driver.runAsync(sql);
    }

    return {
      results: [],
      first: undefined,
    };
  }
}
