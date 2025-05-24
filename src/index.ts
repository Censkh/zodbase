import type * as zod from "zod/v4";
import { findFieldMetaItems } from "zod-meta";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import { escapeSqlValue } from "./Escaping";
import { toLazyPromise } from "./LazyPromise";
import { updatedAt } from "./MetaTypes";
import type {
  BindingKeys,
  FieldBinding,
  InputOfTable,
  SelectCondition,
  SelectQuery,
  SelectQueryBuilder,
  SingleFieldBinding,
  SqlDefiniteResult,
  SqlResult,
  StringKeys,
  ValueOfTable,
} from "./QueryBuilder";
import { type Statement, TO_SQL_SYMBOL, type ToSql } from "./Statement";
import type { Table } from "./Table";
import { isZodRequired } from "./ZodUtils";

export * from "./index.common";
export * from "./ZodUtils";

const IS_REACT_NATIVE = typeof navigator !== "undefined" && (navigator as any).product === "ReactNative";

// @ts-ignore
if (typeof window !== "undefined" && !IS_REACT_NATIVE) {
  throw new Error("[zodbase] This package is not intended for browser usage");
}

export type { InputOfTable } from "./QueryBuilder";

export const valueToSql = (value: any, nested?: boolean): string => {
  if (value?.[TO_SQL_SYMBOL]) {
    return value[TO_SQL_SYMBOL]();
  }

  if (typeof value === "number") {
    return value.toString();
  }

  if (value === "") {
    return value;
  }

  if (Array.isArray(value) && !nested) {
    return `(${value.map((value) => valueToSql(value, true)).join(", ")})`;
  }

  if (typeof value === "undefined" || value === null) {
    return "null";
  }
  if (typeof value === "object") {
    return `json(${escapeSqlValue(JSON.stringify(value))})`;
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  return escapeSqlValue(value?.toString());
};

/*export const upsertSql = <T extends zod.ZodObject<any>>(schema: T, entity: zod.input<T>): string => {
  const tableMeta = getValidMeta(schema, table);
  if (!tableMeta) {
    throw new Error("Table name is required");
  }
  const primaryKeyName = findFieldMeta(schema, primaryKey)?.key;
  const safeEntity = schema.parse(entity);
  return `INSERT INTO ${tableMeta.options.name} (${Object.keys(safeEntity).join(", ")}) VALUES (${Object.values(
    safeEntity,
  ).map((value) => {
    if (typeof value === "string") {
      return `'${value}'`;
    }
    return value;
  })}) ON CONFLICT(${primaryKeyName}) DO UPDATE SET ${SQLITE_ADAPTER.valuesSql(safeEntity, schema)}`;
};

export const updateSql = <T extends zod.ZodObject<any>>(schema: T, entity: Partial<zod.input<T>>): string => {
  const tableMeta = getValidMeta(schema, table);
  if (!tableMeta) {
    throw new Error("Table name is required");
  }
  const primaryKeyName = findFieldMeta(schema, primaryKey)?.key;
  const safeEntity = schema.partial().parse(entity);
  return `UPDATE ${tableMeta.options.name} SET ${SQLITE_ADAPTER.valuesSql(
    safeEntity,
    schema,
  )} WHERE ${primaryKeyName} = '${safeEntity[primaryKeyName]}'`;
};*/

export interface DatabaseOptions {
  adaptor: DatabaseAdaptor;
}

interface BaseFieldModification<M> {
  type: M;
}

export interface AddConstraint extends BaseFieldModification<"add-constraint"> {
  constraint: string;
}

export interface RemoveConstraint extends BaseFieldModification<"remove-constraint"> {
  constraint: string;
}

export type FieldModification = AddConstraint | RemoveConstraint;

export type FieldDiffType = "added" | "removed" | "modified";

export interface FieldDiff {
  key: string;
  field?: SingleFieldBinding;
  type: FieldDiffType;
  modifications?: FieldModification[];
}

export interface TableDiff {
  fields: FieldDiff[];
}

export interface MutationResult<TResult extends SqlResult> extends Promise<SqlResult<void, 0>> {
  selectMutated(): Promise<TResult>;
}

const getFieldBindingsByKeys = <TTable extends Table>(
  table: TTable,
  keys: BindingKeys<ValueOfTable<TTable>>[],
): FieldBinding<ValueOfTable<TTable>>[] => {
  return keys.map<FieldBinding<ValueOfTable<TTable>>>((key) => {
    if (key === "*") {
      return {
        table: table,
        key: "*",
      } as any;
    }
    // @ts-ignore
    return table.fields[key as string];
  });
};

type CountBuilder<TTable extends Table, TResultValue> = Promise<SqlDefiniteResult<TResultValue, 1>> & {
  where(condition: SelectCondition<ValueOfTable<TTable>>): CountBuilder<TTable, TResultValue>;
};

type DeleteBuilder<TTable extends Table> = Promise<SqlResult<void, 0>> & {
  where(condition: SelectCondition<ValueOfTable<TTable>>): DeleteBuilder<TTable>;
};

export const join = (items: ToSql[], separator: string): ToSql => {
  return {
    [TO_SQL_SYMBOL]: () => items.map((item) => item[TO_SQL_SYMBOL]()).join(separator),
  };
};

// tag function for creating sql strings
export function sql(strings: TemplateStringsArray, ...values: any[]): Statement {
  let result = "";
  for (let i = 0; i < strings.length; i++) {
    result += strings[i];
    if (i < values.length) {
      const value = values[i];
      result += valueToSql(value);
    }
  }
  return { [TO_SQL_SYMBOL]: () => result };
}

export const isToSql = (value: any): value is ToSql => {
  return typeof value?.[TO_SQL_SYMBOL] === "function";
};

export function raw(value: string | string[] | Statement | Statement[]): Statement {
  return {
    [TO_SQL_SYMBOL]: () => {
      if (isToSql(value)) {
        return value[TO_SQL_SYMBOL]();
      }

      if (Array.isArray(value)) {
        return value
          .map((subValue) => {
            return isToSql(subValue) ? subValue[TO_SQL_SYMBOL]() : subValue;
          })
          .join(", ");
      }

      return value;
    },
  };
}

const createSelectQueryBuilder = <TTable extends Table, TKey extends BindingKeys<ValueOfTable<TTable>>>(
  query: SelectQuery<TTable>,
  adaptor: DatabaseAdaptor,
): SelectQueryBuilder<TTable, TKey extends "*" ? ValueOfTable<TTable> : Pick<ValueOfTable<TTable>, TKey>, number> => {
  const builder = {
    table: query.table,

    clone() {
      return createSelectQueryBuilder({ ...query }, adaptor);
    },

    where(condition: SelectCondition<ValueOfTable<TTable>>) {
      query.where = condition;
      return this;
    },
    limit<TLimit extends number>(limit: TLimit) {
      query.limit = limit;
      return this;
    },
    offset(offset: number) {
      query.offset = offset;
      return this;
    },
    one() {
      return this.limit(1);
    },
    orderBy(field: SingleFieldBinding<ValueOfTable<TTable>>, direction: "ASC" | "DESC") {
      query.orderBy.push({
        field: field,
        direction,
      });
      return this;
    },
    fields(...fields: TKey[]) {
      query.fields = getFieldBindingsByKeys(query.table, fields) as any;
      return this;
    },

    async execute() {
      return adaptor.executeSelect(query);
    },
    async count() {
      // @ts-ignore
      return adaptor.executeCount(query.table, query.fields, query.where);
    },
  };

  return toLazyPromise(() => adaptor.executeSelect(query), builder) as any;
};

export class Database {
  constructor(private readonly options: DatabaseOptions) {}

  async execute(sql: Statement) {
    const adaptor = this.options.adaptor;
    const result = await adaptor.execute(sql);

    return result;
  }

  select<TTable extends Table, TKey extends BindingKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: TKey[],
  ): SelectQueryBuilder<TTable, TKey extends "*" ? ValueOfTable<TTable> : Pick<ValueOfTable<TTable>, TKey>, number> {
    const query = {
      table: table as any,
      fields: getFieldBindingsByKeys(table, fields) as any,
      where: undefined,
      orderBy: [],
      limit: undefined,
      offset: undefined,
    } as SelectQuery<TTable>;

    return createSelectQueryBuilder(query, this.options.adaptor);
  }

  count<TTable extends Table, TKey extends BindingKeys<ValueOfTable<TTable>>>(table: TTable, ...fields: TKey[]) {
    let where: SelectCondition<ValueOfTable<TTable>> | undefined;
    const adapator = this.options.adaptor;
    const countBuilder = {
      where(condition: SelectCondition<ValueOfTable<TTable>>) {
        where = condition;
        return countBuilder;
      },
    } as CountBuilder<TTable, Record<TKey, number>>;
    return toLazyPromise(
      () => adapator.executeCount(table, getFieldBindingsByKeys(table, fields) as any, where),
      countBuilder,
    );
  }

  delete<TTable extends Table>(table: TTable) {
    let where: SelectCondition<ValueOfTable<TTable>> | undefined;
    const adapator = this.options.adaptor;
    const deleteBuilder = {
      where(condition: SelectCondition<ValueOfTable<TTable>>) {
        where = condition;
        return deleteBuilder;
      },
    } as DeleteBuilder<TTable>;
    return toLazyPromise(() => {
      if (!where) {
        throw new Error("Delete without where condition is not allowed");
      }

      return adapator.executeDelete(table, where);
    }, deleteBuilder);
  }

  async insert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
    return this.options.adaptor.executeInsert(table, values);
  }

  update<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
  ) {
    if (Object.keys(values).length === 0) {
      throw new Error("No values to update");
    }

    const updatedAtFields = findFieldMetaItems(table.schema, updatedAt);
    for (const field of updatedAtFields) {
      // @ts-ignore
      values[field.key] = Date.now();
    }

    const adapator = this.options.adaptor;
    return toLazyPromise(
      (): Promise<SqlResult<void, 0>> => {
        return adapator.executeUpdate(table, values, where);
      },
      {
        async selectMutated<TKey extends BindingKeys<TTable>>(
          ...keys: TKey[]
        ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
          await adapator.executeUpdate(table, values, where);

          return adapator.executeSelect({
            table,
            // @ts-ignore
            fields: getFieldBindingsByKeys(table, keys.length === 0 ? ["*"] : keys),
            where,
            orderBy: [],
            limit: 1,
            offset: undefined,
          }) as any;
        },
      },
    ) satisfies MutationResult<SqlDefiniteResult<ValueOfTable<TTable>, 1>>;
  }

  upsert<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: InputOfTable<TTable>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ) {
    const adaptor = this.options.adaptor;
    return toLazyPromise(
      async () => {
        const result = await adaptor.executeUpsert(table, values, field);
        return result;
      },
      {
        async selectMutated<TKey extends BindingKeys<TTable>>(
          ...keys: TKey[]
        ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
          await adaptor.executeUpsert(table, values, field);
          return adaptor.executeSelect({
            table,
            // @ts-ignore
            fields: getFieldBindingsByKeys(table, keys.length === 0 ? ["*"] : keys),
            where: field.equals((values as any)[field.key]),
            orderBy: [],
            limit: 1,
            offset: undefined,
          }) as any;
        },
      },
    ) satisfies MutationResult<SqlDefiniteResult<ValueOfTable<TTable>, 1>>;
  }

  async updateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TFieldKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TFieldKey>) {
    const adapator = this.options.adaptor;
    return toLazyPromise(
      (): Promise<SqlResult<void, 0>> => {
        if (values.length === 0) {
          return Promise.resolve({
            results: [],
            first: undefined,
          });
        }
        return adapator.executeUpdateMany(table, values, field);
      },
      {
        async selectMutated<TKey extends BindingKeys<TTable>>(
          ...keys: TKey[]
        ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, number>> {
          if (values.length === 0) {
            return {
              results: [],
              first: undefined,
            } as any;
          }
          await adapator.executeUpdateMany(table, values, field);

          return adapator.executeSelect({
            table,
            fields: getFieldBindingsByKeys(table, keys),
            where: field.in(values.map((value) => value[field.key])),
            orderBy: [],
            limit: undefined,
            offset: undefined,
          }) as any;
        },
      },
    ) satisfies MutationResult<SqlDefiniteResult<ValueOfTable<TTable>, number>>;
  }

  async insertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, number>> {
    if (values.length === 0) {
      return {
        results: [],
        first: undefined,
      } as any;
    }

    return this.options.adaptor.executeInsertMany(table, values);
  }

  async syncTable<TTable extends Table>(table: TTable): Promise<void> {
    await this.options.adaptor.createTable(table);

    const diff = await this.diffTable(table);

    if (diff.fields.length > 0) {
      await this.adaptor.processDiff(table, diff);
      //this.options.adaptor.executeSql(sql);
    }
  }

  private get adaptor() {
    return this.options.adaptor;
  }

  private async diffTable<TTable extends Table>(table: TTable): Promise<TableDiff> {
    const tableRemoteSchema = await this.options.adaptor.fetchTableColumns(table);

    const diff: TableDiff = {
      fields: [],
    };

    if (tableRemoteSchema.results.length === 0) {
      return diff;
    }

    for (const column of tableRemoteSchema.results) {
      const field: SingleFieldBinding = (table.fields as any)[column.name];
      if (!field) {
        diff.fields.push({ key: column.name, type: "removed" });
      }
    }

    for (const [key, value] of Object.entries(table.fields)) {
      const field = value as SingleFieldBinding;
      const column = tableRemoteSchema.results.find((column) => column.name === key);
      if (!column) {
        diff.fields.push({
          key: field.key,
          field,
          type: "added",
        });
      } else {
        const isRequired = isZodRequired(field.schema);
        if (column.notNull !== isRequired) {
          diff.fields.push({
            key: field.key,
            field,
            type: "modified",
            modifications: [
              {
                type: isRequired ? "add-constraint" : "remove-constraint",
                constraint: "NOT NULL",
              },
            ],
          });
        }
      }
    }

    return diff;
  }
}

export const mapSqlResult = <TFrom, TTo, TResultLimit extends number>(
  result: SqlResult<TFrom, TResultLimit>,
  mapper: (from: TFrom) => TTo,
): SqlResult<TTo, TResultLimit> => {
  return {
    ...result,
    first: result.first !== undefined ? mapper(result.first as any) : undefined,
    results: result.results.map(mapper),
  } as SqlResult<TTo, TResultLimit>;
};

export { meta } from "zod-meta";
export * from "./MetaTypes";
export type { SelectCondition, SelectQueryBuilder } from "./QueryBuilder";
