import type * as zod from "zod/v4";
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
import type { Statement, ToSql } from "./Statement";

export type StringKeys<T> = {
  [K in keyof T]: K extends string ? K : never;
}[keyof T];

export type BindingKeys<TValue> = "*" | StringKeys<TValue>;

export interface BaseFieldBinding<TValue, TKey extends BindingKeys<TValue>> extends ToSql {
  key: TKey & ToSql;
  table: Table<TValue, string, zod.ZodType<TValue>>;
  schema: zod.ZodType<TValue>;
}

export interface AllFieldsBinding<TValue> extends BaseFieldBinding<TValue, "*"> {}

export interface SingleFieldBinding<TValue = any, TKey extends StringKeys<TValue> = StringKeys<TValue>>
  extends BaseFieldBinding<TValue, TKey> {
  equals(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  like(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  greaterThan(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  lessThan(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  greaterThanOrEquals(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  lessThanOrEquals(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  notEquals(value: TValue[TKey] | undefined | null): SelectFieldCondition<TValue, TKey>;

  in(values: TValue[TKey][]): SelectFieldCondition<TValue, TKey>;
}

export type FieldBinding<TValue> = SingleFieldBinding<TValue, StringKeys<TValue>> | AllFieldsBinding<TValue>;

export type SelectCondition<TValue = any> =
  | SelectFieldCondition<TValue, StringKeys<TValue>>
  | SelectCompoundCondition<TValue>;

export type Falsy = false | null | undefined | "" | 0;

export interface BaseSelectCondition<TValue> {
  and(...condition: Array<SelectCondition<TValue> | Falsy>): SelectCondition<TValue>;

  or(...condition: Array<SelectCondition<TValue> | Falsy>): SelectCondition<TValue>;
}

export interface SelectCompoundCondition<TValue> extends BaseSelectCondition<TValue> {
  type: "AND" | "OR";
  conditions: SelectCondition<TValue>[];
}

export type ValueOfTable<TTable extends Table> = TTable extends Table<infer TValue, any, any> ? TValue : never;
export type InputOfTable<TTable extends Table> = TTable extends Table<any, any, infer TSchema>
  ? zod.input<TSchema>
  : never;

export interface SelectFieldCondition<TValue = any, TKey extends StringKeys<TValue> = StringKeys<TValue>>
  extends BaseSelectCondition<TValue> {
  field: SingleFieldBinding<TValue, TKey>;
  operator: SqlOperator;
  value: TValue[TKey];
}

export const buildConditionSql = (
  adaptor: DatabaseAdaptor,
  condition: SelectCondition,
  doubleQuote?: boolean,
): Statement => {
  if ("conditions" in condition) {
    return sql`(${join(
      condition.conditions.reduce((result, childCondition) => {
        if (childCondition) {
          result.push(buildConditionSql(adaptor, childCondition, doubleQuote));
        }
        return result;
      }, [] as Statement[]),
      ` ${condition.type} `,
    )})`;
  }

  let check = sql`${raw(condition.operator)}
  ${condition.value}`;

  if (condition.operator === "=" && condition.value === null) {
    check = sql`IS NULL`;
  } else if (condition.operator === "!=" && condition.value === null) {
    check = sql`IS NOT NULL`;
  }

  return sql`${raw(condition.field.table.id)}.${
    doubleQuote ? raw(`"${condition.field.key}"`) : raw(condition.field.key)
  } ${check}`;
};

export interface SelectQuery<TTable extends Table = Table, TLimit extends number = number> {
  table: TTable;
  fields: FieldBinding<ValueOfTable<TTable>>[];
  where: SelectCondition<ValueOfTable<TTable>> | undefined;
  orderBy: Array<{
    field: FieldBinding<ValueOfTable<TTable>>;
    direction: OrderDirection;
  }>;
  limit: TLimit | undefined;
  offset: number | undefined;
}

export type SelectQueryBuilder<TTable extends Table, TResultValue, TResultLimit extends number> = Promise<
  SqlResult<TResultValue, TResultLimit>
> & {
  table: TTable;

  fields(...fields: BindingKeys<ValueOfTable<TTable>>[]): SelectQueryBuilder<TTable, TResultValue, TResultLimit>;

  clone(): SelectQueryBuilder<TTable, TResultValue, TResultLimit>;

  where(condition: SelectCondition<ValueOfTable<TTable>>): SelectQueryBuilder<TTable, TResultValue, TResultLimit>;

  limit<TLimit extends number>(limit: TLimit): SelectQueryBuilder<TTable, TResultValue, TLimit>;

  offset(offset: number): SelectQueryBuilder<TTable, TResultValue, TResultLimit>;

  one(): SelectQueryBuilder<TTable, TResultValue, 1>;

  orderBy(
    field: SingleFieldBinding<ValueOfTable<TTable>>,
    direction: OrderDirection,
  ): SelectQueryBuilder<TTable, TResultValue, TResultLimit>;

  count(): Promise<SqlResult<Record<StringKeys<ValueOfTable<TTable>>, number>, 1>>;
};

export type SqlOperator = "=" | "<" | ">" | "<=" | ">=" | "!=" | "LIKE" | "IN";
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
