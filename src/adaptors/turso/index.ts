import type { Client } from "@libsql/client";
import type * as zod from "zod";
import type DatabaseAdaptor from "../../DatabaseAdaptor";
import type { InputOfTable, SingleFieldBinding, SqlResult, StringKeys, ValueOfTable } from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import type { Table } from "../../Table";
import SqliteAdaptor from "../sqlite";

export default class TursoAdaptor extends SqliteAdaptor<Client> {
  protected override async executeTransaction<TResult>(
    callback: (adaptor: DatabaseAdaptor) => Promise<TResult>,
  ): Promise<TResult> {
    const transaction = await this.driver.transaction("write");
    const adaptor = new TursoAdaptor({ ...this.options, driver: transaction as unknown as Client });
    try {
      const result = await callback(adaptor);
      await transaction.commit();
      return result;
    } catch (error) {
      await transaction.rollback();
      throw error;
    } finally {
      transaction.close();
    }
  }

  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const rawSql = statement[TO_SQL_SYMBOL]();
    const res = await this.driver.execute(rawSql);

    return this.mapResult({
      results: res.rows,
      first: res.rows[0],
    });
  }

  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>> {
    const statements = values.map((value) => {
      return this.buildUpdateSql(table, value, field.equals(value[field.key] as any) as any)[TO_SQL_SYMBOL]();
    });

    await this.driver.batch(statements, "write");
    return {
      results: [],
      first: undefined,
    };
  }
}
