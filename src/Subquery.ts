import * as zod from "zod";
import { getMetaItem } from "zod-meta";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import { toLazyPromise } from "./LazyPromise";
import { primaryKey } from "./MetaTypes";
import {
  type InsertValues,
  isScalarSelect,
  type ScalarSubquery,
  SELECT_QUERY,
  type SelectCondition,
  type SelectQuery,
  type SqlResult,
  type ValueOfTable,
  validateSelectCondition,
} from "./QueryBuilder";
import { TO_SQL_SYMBOL } from "./Statement";
import type { Table } from "./Table";

const snapshotValue = (value: unknown): unknown => {
  if (value instanceof Date) return new Date(value.getTime());
  if (Array.isArray(value)) return value.map(snapshotValue);
  if (
    value &&
    typeof value === "object" &&
    !(TO_SQL_SYMBOL in value) &&
    Object.getPrototypeOf(value) === Object.prototype
  )
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, snapshotValue(item)]));
  return value;
};

export const snapshotCondition = (condition: SelectCondition): SelectCondition => {
  condition = validateSelectCondition(condition);
  return "conditions" in condition
    ? { ...condition, conditions: condition.conditions.map(snapshotCondition) }
    : { ...condition, value: snapshotValue(condition.value) };
};

export const snapshotQuery = (query: SelectQuery): SelectQuery => ({
  ...query,
  fields: [...query.fields],
  orderBy: query.orderBy.map((order) => ({ ...order })),
  where: query.where ? snapshotCondition(query.where) : undefined,
  joins: query.joins?.map((join) => ({ ...join, on: snapshotCondition(join.on) })),
  projection: query.projection ? (snapshotValue(query.projection) as SelectQuery["projection"]) : undefined,
  includes: query.includes
    ? Object.fromEntries(Object.entries(query.includes).map(([key, child]) => [key, snapshotQuery(child)]))
    : undefined,
});

export const isSubquery = (value: unknown): value is ScalarSubquery<unknown> =>
  typeof value === "object" && value !== null && SELECT_QUERY in value;
export const hasSubqueries = (rows: object[]) => rows.some((row) => Object.values(row).some(isSubquery));
export interface SubqueryInsertResult<T> extends Promise<SqlResult<void, 0>> {
  selectMutated(): Promise<SqlResult<T>>;
}

export const insertSubqueries = <TTable extends Table>(
  adaptor: DatabaseAdaptor,
  table: TTable,
  rows: InsertValues<TTable>[],
): SubqueryInsertResult<ValueOfTable<TTable>> => {
  if (!(table.schema instanceof zod.ZodObject)) throw new Error("Subquery inserts require an object schema");
  const schema: zod.ZodObject<any> = table.schema;
  const parsedRows = rows.map((row) => {
    const expressions: Record<string, unknown> = {};
    const literals: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      if (!isSubquery(value)) {
        literals[key] = value;
        continue;
      }
      // biome-ignore lint/suspicious/noPrototypeBuiltins: Support consumers targeting pre-ES2022 runtimes.
      if (!Object.prototype.hasOwnProperty.call(schema.shape, key)) throw new Error(`Unknown insert column: ${key}`);
      const query = value[SELECT_QUERY].query;
      if (!isScalarSelect(query)) throw new Error("Scalar subqueries must select exactly one column");
      // Snapshot the AST: later changes to a reusable builder must not change this insert.
      expressions[key] = {
        [SELECT_QUERY]: {
          query: snapshotQuery(query),
        },
      };
    }
    const mask = Object.fromEntries(Object.keys(expressions).map((key) => [key, true as const]));
    return { ...schema.omit(mask).parse(literals), ...expressions };
  });
  let started = false;
  const execute = (returnRows: boolean) => {
    if (started) throw new Error("Insert has already executed");
    started = true;
    return adaptor.executeInsertSubqueries(table, parsedRows, returnRows);
  };
  return toLazyPromise(
    async (): Promise<SqlResult<void, 0>> => {
      await execute(false);
      return { results: [], first: undefined };
    },
    { selectMutated: () => execute(true) as Promise<SqlResult<ValueOfTable<TTable>>> },
  );
};

/** Only share deterministic point reads, never arbitrary/volatile or self-referencing queries. */
export const canShareSubquery = (query: SelectQuery, destination: Table): boolean => {
  if (
    query.joins?.length ||
    query.includes ||
    query.projection ||
    query.table.sourceTable ||
    String(query.table.id) === String(destination.id) ||
    !query.where ||
    query.orderBy.length ||
    query.offset !== undefined
  )
    return false;
  const keys = Object.values(query.table.fields).filter((field) => getMetaItem(field.schema, primaryKey));
  if (keys.length !== 1) return false;
  let hasKey = false;
  const isPointRead = (condition: SelectCondition): boolean => {
    if ("conditions" in condition) return condition.type === "AND" && condition.conditions.every(isPointRead);
    if (condition.field.table !== query.table || !["string", "number", "boolean"].includes(typeof condition.value))
      return false;
    if (condition.field === keys[0] && condition.operator === "=") hasKey = true;
    return true;
  };
  return isPointRead(query.where) && hasKey;
};
