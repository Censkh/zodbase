import type { D1Database, D1PreparedStatement } from "@cloudflare/workers-types";
import type * as zod from "zod";
import type DatabaseAdaptor from "../../DatabaseAdaptor";
import type { DatabaseAdaptorOptions } from "../../DatabaseAdaptor";
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
import { bindLongLiterals } from "./BoundLiterals";

/** Prepares SQL text, binding long string literals so large values fit D1's statement limit. */
const prepare = (driver: D1Database, rawSql: string): D1PreparedStatement => {
  const { sql, params } = bindLongLiterals(rawSql);
  const statement = driver.prepare(sql);
  return params.length > 0 ? statement.bind(...params) : statement;
};

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

  /** D1 also keeps its own `_cf_` tables, which queries can't read. */
  protected override isInternalTable(name: string): boolean {
    return super.isInternalTable(name) || name.startsWith("_cf_");
  }

  // One D1 batch: a single round trip for all of them.
  protected override async executeReads(statements: Statement[]): Promise<SqlResult[]> {
    if (statements.length === 0) return [];
    const results = await this.driver.batch(
      statements.map((statement) => prepare(this.driver, this.sqlText(statement))),
    );
    return results.map(({ results }) => this.mapResult({ results, first: results[0] }));
  }

  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const rawSql = this.sqlText(statement);
    const preparedStatement = prepare(this.driver, rawSql);
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
  >(
    table: TTable,
    values: TValue[],
    fields: readonly SingleFieldBinding<TValue, TKey>[],
    where?: SelectCondition,
  ): Promise<SqlResult<void, 0>> {
    const statements = values.map((value) => {
      return prepare(
        this.driver,
        this.sqlText(this.buildUpdateSql(table, value, keyCondition(fields, value, where) as any)),
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
    const rawSql = this.sqlText(statement);
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
    this.statements.push(prepare(this.driver, rawSql));
    return { results: [], first: undefined };
  }

  override async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(
    table: TTable,
    values: TValue[],
    fields: readonly SingleFieldBinding<TValue, TKey>[],
    where?: SelectCondition,
  ): Promise<SqlResult<void, 0>> {
    for (const value of values) {
      await this.execute(this.buildUpdateSql(table, value, keyCondition(fields, value, where) as any));
    }
    return { results: [], first: undefined };
  }

  protected override executeTransaction<TResult>(
    _callback: (adaptor: DatabaseAdaptor) => Promise<TResult>,
  ): Promise<TResult> {
    throw new Error("Nested D1 transactions are not supported");
  }
}
