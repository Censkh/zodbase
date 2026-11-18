import type { Client } from "@libsql/client";
import type * as zod from "zod";
import type DatabaseAdaptor from "../../DatabaseAdaptor";
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
import { exactFloatLiterals } from "./ExactFloats";

export default class TursoAdaptor extends SqliteAdaptor<Client> {
  protected override async executeTransaction<TResult>(
    callback: (adaptor: DatabaseAdaptor) => Promise<TResult>,
  ): Promise<TResult> {
    const transaction = await this.driver.transaction("write");
    const adaptor = new TursoAdaptor(this.optionsFor(transaction as unknown as Client));
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

  // libsql stores a NUL character in text but reads it back as the text's end, so it refuses one.
  protected override sqlText(statement: Statement): string {
    const text = statement[TO_SQL_SYMBOL]();
    if (text.includes("\0")) throw new Error("[zodbase] libsql can't read back a NUL character in text");
    return exactFloatLiterals(text);
  }

  // One libSQL batch: a single round trip for all of them.
  protected override async executeReads(statements: Statement[]): Promise<SqlResult[]> {
    if (statements.length === 0) return [];
    const results = await this.driver.batch(
      statements.map((statement) => this.sqlText(statement)),
      "read",
    );
    return results.map((result) => this.mapResult({ results: result.rows, first: result.rows[0] }));
  }

  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const res = await this.driver.execute(this.sqlText(statement));

    return this.mapResult({
      results: res.rows,
      first: res.rows[0],
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
      return this.sqlText(this.buildUpdateSql(table, value, keyCondition(fields, value, where) as any));
    });

    await this.driver.batch(statements, "write");
    return {
      results: [],
      first: undefined,
    };
  }
}
