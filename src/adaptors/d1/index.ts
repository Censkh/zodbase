import type { D1Database, D1PreparedStatement } from "@cloudflare/workers-types";
import type * as zod from "zod";
import type DatabaseAdaptor from "../../DatabaseAdaptor";
import type { DatabaseAdaptorOptions } from "../../DatabaseAdaptor";
import type { InputOfTable, SingleFieldBinding, SqlResult, StringKeys, ValueOfTable } from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import type { Table } from "../../Table";
import SqliteAdaptor from "../sqlite";

export default class D1Adaptor extends SqliteAdaptor<D1Database> {
  protected override async executeTransaction<TResult>(
    callback: (adaptor: DatabaseAdaptor) => Promise<TResult>,
  ): Promise<TResult> {
    const statements: D1PreparedStatement[] = [];
    const transactionAdaptor = new D1BatchTransactionAdaptor(this.options, statements);
    const result = await callback(transactionAdaptor);
    if (statements.length > 0) {
      await this.driver.batch(statements);
    }
    return result;
  }

  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const rawSql = statement[TO_SQL_SYMBOL]();
    const preparedStatement = this.driver.prepare(rawSql);
    const normalizedSql = rawSql.trim().toUpperCase();
    const returnsRows =
      normalizedSql.startsWith("SELECT") ||
      normalizedSql.startsWith("PRAGMA") ||
      normalizedSql.startsWith("EXPLAIN") ||
      /\bRETURNING\b/.test(normalizedSql);
    const res = returnsRows ? await preparedStatement.all() : await preparedStatement.run();

    return this.mapResult({
      results: res.results,
      first: res.results[0],
    });
  }

  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>> {
    const statements = values.map((value) => {
      return this.driver.prepare(
        this.buildUpdateSql(table, value, field.equals(value[field.key] as any) as any)[TO_SQL_SYMBOL](),
      );
    });

    await this.driver.batch(statements);
    return {
      results: [],
      first: undefined,
    };
  }
}

class D1BatchTransactionAdaptor extends D1Adaptor {
  constructor(
    options: DatabaseAdaptorOptions<D1Database>,
    private readonly statements: D1PreparedStatement[],
  ) {
    super(options);
  }

  override async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }
    const rawSql = statement[TO_SQL_SYMBOL]();
    const normalizedSql = rawSql.trim().toUpperCase();
    if (
      normalizedSql.startsWith("SELECT") ||
      normalizedSql.startsWith("PRAGMA") ||
      normalizedSql.startsWith("EXPLAIN") ||
      normalizedSql.startsWith("WITH") ||
      /\bRETURNING\b/.test(normalizedSql)
    ) {
      throw new Error("D1 transactions support write operations only");
    }
    this.statements.push(this.driver.prepare(rawSql));
    return { results: [], first: undefined };
  }

  override async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>> {
    for (const value of values) {
      await this.execute(this.buildUpdateSql(table, value, field.equals(value[field.key] as any) as any));
    }
    return { results: [], first: undefined };
  }

  protected override executeTransaction<TResult>(
    _callback: (adaptor: DatabaseAdaptor) => Promise<TResult>,
  ): Promise<TResult> {
    throw new Error("Nested D1 transactions are not supported");
  }
}
