import type { Database as BunDatabase } from "bun:sqlite";
import type * as zod from "zod";
import { keyCondition } from "../../DatabaseAdaptor";
import type { Table } from "../../index";
import type {
  InputOfTable,
  SelectCondition,
  SingleFieldBinding,
  SqlResult,
  StringKeys,
  ValueOfTable,
} from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import SqliteAdaptor from "../sqlite";

export default class BunSqliteAdaptor extends SqliteAdaptor<BunDatabase> {
  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }
    const rawSql = statement[TO_SQL_SYMBOL]();
    const preparedStatement = this.driver.prepare(rawSql);

    const res = await preparedStatement.all();

    return this.mapResult({
      results: res,
      first: res[0],
    });
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
    const statements = values.map((value) => {
      return this.buildUpdateSql(table, value, keyCondition(fields, value, where) as any);
    });
    await Promise.all(statements.map((statement) => this.execute(statement)));
    return {
      results: [],
      first: undefined,
    };
  }
}
