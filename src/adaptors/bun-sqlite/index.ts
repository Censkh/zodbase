import type { Database as BunDatabase } from "bun:sqlite";
import type * as zod from "zod";
import type {
  InputOfTable,
  SingleFieldBinding,
  SqlResult,
  StringKeys,
  ValueOfTable,
} from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import type { Table } from "../../index";
import SqliteAdaptor from "../sqlite";

export default class BunSqliteAdaptor extends SqliteAdaptor<BunDatabase> {
  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }
    const rawSql = statement[TO_SQL_SYMBOL]();
    //console.log(rawSql);
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
    field: SingleFieldBinding<TValue, TKey>,
  ): Promise<SqlResult<void, 0>> {
    /*const statements = values.map((value) => {
      return this.driver.prepare(
        this.buildUpdateSql(table, value, field.equals(value[field.key] as any)),
      );
    });
    //const res = await this.driver.batch(statements);*/
    return {
      results: [],
      first: undefined,
    };
  }
}
