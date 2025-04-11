import type * as zod from "zod";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import type { Statement, ToSql } from "./Statement";
//import {Table} from "src/zodbase/index.ts";
/*export class QueryBuilder<T> {
  where(field: keyof T): this {
    return this;
  }
}

export const query = <T extends object>(table: Table<T>) => QueryBuilder<T> => {

}
*/
import { type Table, join, raw, sql } from "./index";

export type StringKeys<T> = {
  [K in keyof T]: K extends string ? K : never;
}[keyof T];

export type BindingKeys<TValue> = "*" | StringKeys<TValue>;

export interface BaseFieldBinding<TValue extends zod.ZodRawShape, TKey extends BindingKeys<TValue>> extends ToSql {
  key: TKey & ToSql;
  table: Table<TValue, string, zod.ZodObject<TValue>>;
  schema: zod.ZodType<TValue>;
}

export interface AllFieldsBinding<TValue extends zod.ZodRawShape> extends BaseFieldBinding<TValue, "*"> {}

export interface SingleFieldBinding<
  TValue extends zod.ZodRawShape = any,
  TKey extends StringKeys<TValue> = StringKeys<TValue>,
> extends BaseFieldBinding<TValue, TKey> {
  equals(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  like(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  greaterThan(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  lessThan(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  greaterThanOrEquals(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  lessThanOrEquals(value: TValue[TKey]): SelectFieldCondition<TValue, TKey>;

  notEquals(value: TValue[TKey] | null): SelectFieldCondition<TValue, TKey>;

  in(values: TValue[TKey][]): SelectFieldCondition<TValue, TKey>;
}

export type FieldBinding<TValue extends zod.ZodRawShape> =
  | SingleFieldBinding<TValue, StringKeys<TValue>>
  | AllFieldsBinding<TValue>;

export type SelectCondition<TValue extends zod.ZodRawShape = any> =
  | SelectFieldCondition<TValue, StringKeys<TValue>>
  | SelectCompoundCondition<TValue>;

export interface BaseSelectCondition<TValue extends zod.ZodRawShape> {
  and(...condition: SelectCondition<TValue>[]): SelectCondition<TValue>;

  or(...condition: SelectCondition<TValue>[]): SelectCondition<TValue>;
}

export interface SelectCompoundCondition<TValue extends zod.ZodRawShape> extends BaseSelectCondition<TValue> {
  type: "AND" | "OR";
  conditions: SelectCondition<TValue>[];
}

export type ValueOfTable<TTable extends Table> = TTable extends Table<infer TValue, any, any> ? TValue : never;
export type InputOfTable<TTable extends Table> = TTable extends Table<any, any, infer TSchema>
  ? zod.input<TSchema>
  : never;

export interface SelectFieldCondition<
  TValue extends zod.ZodRawShape = any,
  TKey extends StringKeys<TValue> = StringKeys<TValue>,
> extends BaseSelectCondition<TValue> {
  field: SingleFieldBinding<TValue, TKey>;
  operator: SqlOperator;
  value: TValue[TKey];
}

export const buildConditionSql = (adaptor: DatabaseAdaptor, condition: SelectCondition): Statement => {
  if ("conditions" in condition) {
    return sql`${join(
      condition.conditions.map((childCondition) => buildConditionSql(adaptor, childCondition)),
      ` ${condition.type} `,
    )}`;
  }

  let check = sql`${raw(condition.operator)} ${condition.value}`;

  if (condition.operator === "=" && condition.value === null) {
    check = sql`IS NULL`;
  } else if (condition.operator === "!=" && condition.value === null) {
    check = sql`IS NOT NULL`;
  }

  return sql`${raw(condition.field.table.id)}.${raw(condition.field.key)} ${check}`;
};

export interface SelectQuery<TTable extends Table = Table, TLimit extends number = number> {
  table: TTable;
  fields: FieldBinding<ValueOfTable<TTable>>[];
  where: SelectCondition<ValueOfTable<TTable>> | undefined;
  orderBy: Array<{
    field: FieldBinding<ValueOfTable<TTable>>;
    direction: "ASC" | "DESC";
  }>;
  limit: TLimit | undefined;
}

export type SelectQueryBuilder<TTable extends Table, TResultValue, TResultLimit extends number> = Promise<
  SqlResult<TResultValue, TResultLimit>
> & {
  where(condition: SelectCondition<ValueOfTable<TTable>>): SelectQueryBuilder<TTable, TResultValue, TResultLimit>;

  limit<TLimit extends number>(limit: TLimit): SelectQueryBuilder<TTable, TResultValue, TLimit>;

  one(): SelectQueryBuilder<TTable, TResultValue, 1>;

  orderBy(
    field: SingleFieldBinding<ValueOfTable<TTable>>,
    direction: "ASC" | "DESC",
  ): SelectQueryBuilder<TTable, TResultValue, TResultLimit>;
};

export type SqlOperator = "=" | "<" | ">" | "<=" | ">=" | "!=" | "LIKE" | "IN";
export type StringOrNever<T> = T extends string ? T : never;

export interface SqlResult<TValue = any, TLimit extends number = number> {
  results: TLimit extends 0 ? [] : TValue[];
  first: TLimit extends 0 ? void : TValue | undefined;
  limit?: TLimit;
}

export interface SqlDefiniteResult<TValue, TLimit extends number> extends Omit<SqlResult<TValue, TLimit>, "first"> {
  first: TValue;
}

//result.users;
