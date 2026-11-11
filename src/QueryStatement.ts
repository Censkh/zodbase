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

export const selectStatement = (builder: { [SELECT_QUERY]: { query: SelectQuery } }) => {
  const query = snapshotQuery(builder[SELECT_QUERY].query);
  assertSelectArguments(query);
  return new QueryStatement((adaptor) => adaptor.buildSelectSql(query, false, true));
};

export const union = (first: QueryStatement, second: QueryStatement) =>
  new QueryStatement((adaptor) => sql`${first.compile(adaptor)} UNION ${second.compile(adaptor)}`);

/** The table definitions supply the names and column bindings for references to each CTE. */
export const withCtes = (
  entries: readonly { table: Table; query: QueryStatement }[],
  body: QueryStatement,
  options: { recursive?: boolean } = {},
) =>
  new QueryStatement((adaptor) => {
    if (!entries.length) return body.compile(adaptor);
    const definitions = entries.map(({ table, query }) => sql`${table.id} AS (${query.compile(adaptor)})`);
    return sql`WITH ${raw(options.recursive ? "RECURSIVE " : "")}${join(definitions, ", ")} ${body.compile(adaptor)}`;
  });
