import type { Database } from "@libsql/client";
import type * as zod from "zod/v4";
import type { InputOfTable, SingleFieldBinding, SqlResult, StringKeys, ValueOfTable } from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import type { Table } from "../../Table";
import SqliteAdaptor from "../sqlite";

export default class TursoAdaptor extends SqliteAdaptor<Database> {
  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const startTimestamp = Date.now();
    const rawSql = statement[TO_SQL_SYMBOL]();
    const res = await this.driver.execute(rawSql);

    if (this.options.debug) {
      console.debug("TursoAdaptor.execute", "Executed SQL", {
        sql: rawSql,
        timings: {
          totalDurationMs: Date.now() - startTimestamp,
        },
      });
    }

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

    await this.driver.batch(statements);
    return {
      results: [],
      first: undefined,
    };
  }
} 