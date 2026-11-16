import type DatabaseAdaptor from "./DatabaseAdaptor";
import { join, raw, sql } from "./index";
import { assertSelectArguments, SELECT_QUERY, type SelectQuery } from "./QueryBuilder";
import type { Statement } from "./Statement";
import { snapshotQuery } from "./Subquery";
import type { Table } from "./Table";

/** A statement compiled with the executing database's SQL dialect. */
export class QueryStatement {
  constructor(readonly compile: (adaptor: DatabaseAdaptor) => Statement) {}
}

/** Builders that compile to a statement: inserts, updates and deletes (`Writes`). */
export const COMPILE = Symbol("zodbase.compile");

/**
 * Anything that compiles to a statement for the executing database: a query statement (`union`,
 * `withCtes`), a select builder, or an insert, update or delete builder. Every API that takes a
 * statement (`withCtes`, `union`, `exists`, `in`, `scalar`, `execute`, `compile`) takes any of them.
 */
export type Compilable =
  | QueryStatement
  | { [SELECT_QUERY]: { query: SelectQuery } }
  | { [COMPILE](adaptor: DatabaseAdaptor): Statement };

export const isCompilable = (value: unknown): value is Compilable =>
  value instanceof QueryStatement ||
  (typeof value === "object" && value !== null && (SELECT_QUERY in value || COMPILE in value));

/**
 * The statement a compilable compiles to. A select is snapshotted now, so later changes to its
 * builder don't change the statement.
 */
export const toQueryStatement = (value: Compilable): QueryStatement => {
  if (value instanceof QueryStatement) return value;
  if (SELECT_QUERY in value) {
    const query = snapshotQuery(value[SELECT_QUERY].query);
    assertSelectArguments(query);
    return new QueryStatement((adaptor) => adaptor.buildSelectSql(query, false, true));
  }
  return new QueryStatement((adaptor) => value[COMPILE](adaptor));
};

export const union = (first: Compilable, second: Compilable) => {
  const [left, right] = [toQueryStatement(first), toQueryStatement(second)];
  return new QueryStatement((adaptor) => sql`${left.compile(adaptor)} UNION ${right.compile(adaptor)}`);
};

/** The table definitions supply the names and column bindings for references to each CTE. */
export const withCtes = (
  entries: readonly { table: Table; query: Compilable }[],
  body: Compilable,
  options: { recursive?: boolean } = {},
) => {
  const compiledBody = toQueryStatement(body);
  const compiledEntries = entries.map(({ table, query }) => ({ table, query: toQueryStatement(query) }));
  return new QueryStatement((adaptor) => {
    if (!compiledEntries.length) return compiledBody.compile(adaptor);
    const definitions = compiledEntries.map(({ table, query }) => sql`${table.id} AS (${query.compile(adaptor)})`);
    return sql`WITH ${raw(options.recursive ? "RECURSIVE " : "")}${join(definitions, ", ")} ${compiledBody.compile(adaptor)}`;
  });
};
