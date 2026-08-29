import type * as zod from "zod/v4";
import type { InputOfTable, SingleFieldBinding, SqlResult, StringKeys, ValueOfTable } from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import type { Table } from "../../Table";
import SqliteAdaptor from "../sqlite";

export interface BetterSqlite3Database {
  prepare(sql: string): {
    readonly reader: boolean;
    all(...parameters: any[]): unknown[];
    run(...parameters: any[]): unknown;
  };
}

export default class BetterSqlite3Adaptor extends SqliteAdaptor<BetterSqlite3Database> {
  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const preparedStatement = this.driver.prepare(statement[TO_SQL_SYMBOL]());
    let rows: unknown[] = [];
    if (preparedStatement.reader) {
      rows = preparedStatement.all();
    } else {
      preparedStatement.run();
    }
    return this.mapResult({
      results: rows,
      first: rows[0],
    });
  }

  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>> {
    for (const value of values) {
      await this.execute(this.buildUpdateSql(table, value, field.equals(value[field.key] as any) as any));
    }

    return {
      results: [],
      first: undefined,
    };
  }
}
