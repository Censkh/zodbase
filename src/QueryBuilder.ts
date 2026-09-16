import type * as zod from "zod";
import type DatabaseAdaptor from "./DatabaseAdaptor";
//import {Table} from "src/zodbase/index.ts";
/*export class QueryBuilder<T> {
  where(field: keyof T): this {
    return this;
  }
}

export const query = <T extends object>(table: Table<T>) => QueryBuilder<T> => {

}
*/
import { join, type OrderDirection, raw, sql, type Table } from "./index";
import { SqlExpression } from "./SqlExpression";
import { type Statement, TO_SQL_SYMBOL, type ToSql } from "./Statement";

export const SELECT_QUERY = Symbol("selectQuery");

/** A SELECT usable as one SQL value; only single-column projections qualify. */
export interface ScalarSubquery<T> {
  readonly [SELECT_QUERY]: { query: SelectQuery; value: T };
}
type IsUnion<T, U = T> = T extends U ? ([U] extends [T] ? false : true) : never;
type ScalarValue<T> = ScalarSubquery<
  true extends IsUnion<keyof T> ? { readonly multipleColumns: unique symbol } : T[keyof T]
>;

export type InsertValues<TTable extends Table> = {
  [K in keyof InputOfTable<TTable>]: InputOfTable<TTable>[K] | ScalarSubquery<InputOfTable<TTable>[K]>;
};

export type StringKeys<T> = {
  [K in keyof T]-?: K extends string ? K : never;
}[keyof T];

export type BindingKeys<TValue> = "*" | StringKeys<TValue>;

export interface BaseFieldBinding<TValue, TKey extends BindingKeys<TValue>, TName extends string = string>
  extends ToSql {
  key: TKey & ToSql;
  table: Table<TValue, TName, zod.ZodType<TValue>>;
  schema: zod.ZodType;
}

export interface AllFieldsBinding<TValue> extends BaseFieldBinding<TValue, "*"> {}

export interface FieldReference<T> {
  readonly key: string & ToSql;
  readonly table: Table;
  readonly schema: zod.ZodType<T>;
}

export interface SingleFieldBinding<
  TValue = any,
  TKey extends StringKeys<TValue> = StringKeys<TValue>,
  TName extends string = string,
> extends BaseFieldBinding<TValue, TKey, TName> {
  schema: zod.ZodType<TValue[TKey]>;
  equals(value: TValue[TKey] | FieldReference<TValue[TKey] | null | undefined>): SelectFieldCondition<TValue, TKey>;

  like(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  greaterThan(
    value: TValue[TKey] | FieldReference<TValue[TKey] | null | undefined>,
  ): SelectFieldCondition<TValue, TKey>;

  lessThan(value: TValue[TKey] | FieldReference<TValue[TKey] | null | undefined>): SelectFieldCondition<TValue, TKey>;

  greaterThanOrEquals(
    value: TValue[TKey] | FieldReference<TValue[TKey] | null | undefined>,
  ): SelectFieldCondition<TValue, TKey>;

  lessThanOrEquals(
    value: TValue[TKey] | FieldReference<TValue[TKey] | null | undefined>,
  ): SelectFieldCondition<TValue, TKey>;

  notEquals(
    value: TValue[TKey] | FieldReference<TValue[TKey] | null | undefined> | undefined | null,
  ): SelectFieldCondition<TValue, TKey>;

  in(values: TValue[TKey][]): SelectFieldCondition<TValue, TKey>;

  notIn(values: TValue[TKey][]): SelectFieldCondition<TValue, TKey>;

  contains(value: unknown): SelectFieldCondition<TValue, TKey>;
}

export type FieldBinding<TValue> = SingleFieldBinding<TValue, StringKeys<TValue>> | AllFieldsBinding<TValue>;

export type SelectCondition<TValue = any> =
  | SelectFieldCondition<TValue, StringKeys<TValue>>
  | SelectCompoundCondition<TValue>;

export type Falsy = false | null | undefined | "" | 0;

export interface BaseSelectCondition<TValue> {
  and(...condition: Array<SelectCondition | Falsy>): SelectCondition<TValue>;

  or(...condition: Array<SelectCondition | Falsy>): SelectCondition<TValue>;
}

export interface SelectCompoundCondition<TValue> extends BaseSelectCondition<TValue> {
  type: "AND" | "OR";
  conditions: SelectCondition<TValue>[];
}

export type ValueOfTable<TTable extends Table> = TTable extends Table<infer TValue, any, any> ? TValue : never;
export type InputOfTable<TTable extends Table> =
  TTable extends Table<any, any, infer TSchema> ? zod.input<TSchema> : never;

export interface SelectFieldCondition<TValue = any, TKey extends StringKeys<TValue> = StringKeys<TValue>>
  extends BaseSelectCondition<TValue> {
  field: SingleFieldBinding<TValue, TKey>;
  operator: SqlOperator;
  value: TValue[TKey];
}

export const isFieldReference = (value: unknown): value is SingleFieldBinding =>
  typeof value === "object" &&
  value !== null &&
  TO_SQL_SYMBOL in value &&
  "table" in value &&
  "key" in value &&
  "schema" in value;

export const buildConditionSql = (
  adaptor: DatabaseAdaptor,
  condition: SelectCondition,
  options?: boolean | { doubleQuote?: boolean; includeTable?: boolean },
): Statement => {
  const includeTable = typeof options === "boolean" ? true : (options?.includeTable ?? true);

  if ("conditions" in condition) {
    return sql`(${join(
      condition.conditions.reduce((result, childCondition) => {
        if (childCondition) {
          result.push(buildConditionSql(adaptor, childCondition, { includeTable }));
        }
        return result;
      }, [] as Statement[]),
      ` ${condition.type} `,
    )})`;
  }

  const fieldSql = `${includeTable ? `${adaptor.quoteIdentifier(String(condition.field.table.id))}.` : ""}${adaptor.quoteIdentifier(String(condition.field.key))}`;

  if (condition.operator === "JSON_CONTAINS") {
    return adaptor.buildJsonArrayContainsSql(fieldSql, condition.value);
  }

  if ((condition.operator === "IN" || condition.operator === "NOT IN") && condition.value.length === 0) {
    return sql`${raw(condition.operator === "IN" ? "1 = 0" : "1 = 1")}`;
  }

  let check = sql`${raw(condition.operator)}
  ${raw(isFieldReference(condition.value) ? `${adaptor.quoteIdentifier(String(condition.value.table.id))}.${adaptor.quoteIdentifier(String(condition.value.key))}` : adaptor.valueToSql(condition.value))}`;

  if (condition.operator === "=" && condition.value === null) {
    check = sql`IS NULL`;
  } else if (condition.operator === "!=" && condition.value === null) {
    check = sql`IS NOT NULL`;
  }

  return sql`${raw(fieldSql)} ${check}`;
};

export interface SelectQuery<TTable extends Table = Table, TLimit extends number = number> {
  table: TTable;
  joins?: { type: "LEFT" | "INNER"; table: Table; on: SelectCondition }[];
  projection?: Selection;
  includes?: Record<string, SelectQuery>;
  fields: FieldBinding<ValueOfTable<TTable>>[];
  where: SelectCondition<ValueOfTable<TTable>> | undefined;
  orderBy: Array<{
    field: FieldBinding<ValueOfTable<TTable>>;
    direction: OrderDirection;
  }>;
  limit: TLimit | undefined;
  offset: number | undefined;
}

export interface Selection {
  [key: string]: Table | SingleFieldBinding | SqlExpression | Selection;
}
type SelectedTables<P> =
  P extends Table<any, infer N>
    ? N
    : P extends SingleFieldBinding<any, any, infer N>
      ? N
      : P extends Selection
        ? { [K in keyof P]: SelectedTables<P[K]> }[keyof P]
        : never;
type NullableSelection<P, N, R> = [SelectedTables<P>] extends [never]
  ? R
  : true extends IsUnion<SelectedTables<P>>
    ? R
    : SelectedTables<P> extends N
      ? R | null
      : R;
export type SelectionValue<P, N = never> =
  P extends SqlExpression<infer S>
    ? zod.infer<S>
    : P extends Table<infer V, infer Name>
      ? Name extends N
        ? V | null
        : V
      : P extends SingleFieldBinding<infer V, infer K, infer Name>
        ? Name extends N
          ? V[K] | null
          : V[K]
        : P extends Selection
          ? NullableSelection<P, N, { [K in keyof P]: SelectionValue<P[K], SelectedTables<P> extends N ? never : N> }>
          : never;
export type IncludeQuery = Promise<SqlResult<any>> & { readonly [SELECT_QUERY]: { query: SelectQuery } };
type JoinSelection<R, P, N> = P extends Selection ? Omit<R, keyof P> & { [K in keyof P]: SelectionValue<P[K], N> } : R;
type IncludedValue<Q> = Q extends Promise<SqlResult<infer V, any>> ? V[] : never;
type IncludeValues<I> = { [K in keyof I]: IncludedValue<I[K]> };
type NonScalarSelection = { readonly nonScalarSelection: unique symbol };
type QueryScalarValue<R, P, I> = keyof I extends never
  ? P extends undefined
    ? ScalarValue<R>
    : P extends Selection
      ? P[keyof P] extends SingleFieldBinding | SqlExpression
        ? ScalarValue<R>
        : ScalarSubquery<NonScalarSelection>
      : ScalarSubquery<NonScalarSelection>
  : ScalarSubquery<NonScalarSelection>;

export const isScalarSelect = (query: SelectQuery): boolean => {
  if (query.includes && Object.keys(query.includes).length) return false;
  if (query.projection) {
    const values = Object.values(query.projection);
    return values.length === 1 && (isFieldReference(values[0]) || values[0] instanceof SqlExpression);
  }
  return query.fields.length === 1 && query.fields[0]?.key !== "*";
};

export type SelectQueryBuilder<
  TTable extends Table,
  TResultValue,
  TResultLimit extends number,
  TNullable extends string = never,
  TProjection extends Selection | undefined = undefined,
  TIncludes extends Record<string, IncludeQuery> = {},
> = Promise<SqlResult<TResultValue, TResultLimit>> &
  QueryScalarValue<TResultValue, TProjection, TIncludes> & {
    table: TTable;
    leftJoin<T extends Table>(
      table: T,
      on: SelectCondition,
    ): SelectQueryBuilder<
      TTable,
      JoinSelection<TResultValue, TProjection, TNullable | (T extends Table<any, infer N> ? N : never)>,
      TResultLimit,
      TNullable | (T extends Table<any, infer N> ? N : never),
      TProjection,
      TIncludes
    >;
    innerJoin<T extends Table>(
      table: T,
      on: SelectCondition,
    ): SelectQueryBuilder<TTable, TResultValue, TResultLimit, TNullable, TProjection, TIncludes>;
    include<I extends Record<string, IncludeQuery>>(
      includes: I,
    ): SelectQueryBuilder<
      TTable,
      Omit<TResultValue, keyof I> & IncludeValues<I>,
      TResultLimit,
      TNullable,
      TProjection,
      Omit<TIncludes, keyof I> & I
    >;
    clone(): SelectQueryBuilder<TTable, TResultValue, TResultLimit, TNullable, TProjection, TIncludes>;
    where(
      condition: SelectCondition,
    ): SelectQueryBuilder<TTable, TResultValue, TResultLimit, TNullable, TProjection, TIncludes>;
    limit<TLimit extends number>(
      limit: TLimit,
    ): SelectQueryBuilder<TTable, TResultValue, TLimit, TNullable, TProjection, TIncludes>;
    offset(offset: number): SelectQueryBuilder<TTable, TResultValue, TResultLimit, TNullable, TProjection, TIncludes>;
    one(): SelectQueryBuilder<TTable, TResultValue, 1, TNullable, TProjection, TIncludes>;
    orderBy(
      field: SingleFieldBinding,
      direction: OrderDirection,
    ): SelectQueryBuilder<TTable, TResultValue, TResultLimit, TNullable, TProjection, TIncludes>;
    count(): Promise<SqlResult<Record<StringKeys<ValueOfTable<TTable>>, number>, 1>>;
  };

export type SqlOperator = "=" | "<" | ">" | "<=" | ">=" | "!=" | "LIKE" | "IN" | "NOT IN" | "JSON_CONTAINS";
export type StringOrNever<T> = T extends string ? T : never;

export interface SqlResultTimings {
  wallTimeMs: number;
  databaseTimeMs?: number;
}

export interface SqlResult<TValue = any, TLimit extends number = number> {
  results: TLimit extends 0 ? [] : TValue[];
  first: TLimit extends 0 ? void : TValue | undefined;
  limit?: TLimit;
  timings?: SqlResultTimings;
}

export interface SqlDefiniteResult<TValue, TLimit extends number> extends Omit<SqlResult<TValue, TLimit>, "first"> {
  first: TValue;
}

//result.users;
