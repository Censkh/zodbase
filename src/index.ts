import * as zod from "zod/v4";
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
  SqlResultTimings,
  StringKeys,
  ValueOfTable,
} from "./QueryBuilder";
import { type Statement, TO_SQL_SYMBOL, type ToSql } from "./Statement";
import type { Table } from "./Table";
import { isZodRequired } from "./ZodUtils";

export * from "./index.common";
export * from "./ZodUtils";
export { TO_SQL_SYMBOL };

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
    const objectString = escapeSqlValue(JSON.stringify(value));
    return nested ? objectString : `(${objectString})`;
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

export interface ExecuteStatementEvent {
  success: boolean;
  timings: SqlResultTimings;
  sql: string;
}

export interface DatabaseEvents {
  onExecuteStatement?: (event: ExecuteStatementEvent) => void;
}

export interface DatabaseOptions {
  adaptor: DatabaseAdaptor;
  events?: DatabaseEvents;
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
    let fieldBindings: any = getFieldBindingsByKeys(table, fields);
    if (fieldBindings.length === 0) {
      fieldBindings = [
        {
          table: table,
          key: "*",
          schema: zod.number(),
        },
      ];
    }
    return toLazyPromise(() => adapator.executeCount(table, fieldBindings, where), countBuilder);
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
    const parsedValues = table.schema.parse(values);
    return this.options.adaptor.executeInsert(table, parsedValues as any);
  }

  update<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
  ) {
    if (Object.keys(values).length === 0) {
      throw new Error("No values to update");
    }

    const parsedValues: any = (table.schema as zod.ZodObject<any>).partial().parse(values);

    const updatedAtFields = findFieldMetaItems(table.schema, updatedAt);
    for (const field of updatedAtFields) {
      // @ts-ignore
      parsedValues[field.key] = Date.now();
    }

    const adapator = this.options.adaptor;
    return toLazyPromise(
      async (): Promise<SqlResult<void, 0>> => {
        await adapator.executeUpdate(table, parsedValues, where, false);
        return {
          results: [],
          first: undefined,
        };
      },
      {
        async selectMutated<TKey extends BindingKeys<TTable>>(
          ...keys: TKey[]
        ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
          const result = await adapator.executeUpdate(table, parsedValues, where, true);
          if (result.results.length > 0) {
            return result as any;
          }

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
    const parsedValues: any = (table.schema as zod.ZodObject<any>).parse(values);
    const adaptor = this.options.adaptor;
    return toLazyPromise(
      async () => {
        await adaptor.executeUpsert(table, parsedValues, field);
        return {
          results: [],
          first: undefined,
        };
      },
      {
        async selectMutated<TKey extends BindingKeys<TTable>>(
          ...keys: TKey[]
        ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
          const result = await adaptor.executeUpsert(table, parsedValues, field);
          if (result.results.length > 0) {
            return result as any;
          }

          return adaptor.executeSelect({
            table,
            // @ts-ignore
            fields: getFieldBindingsByKeys(table, keys.length === 0 ? ["*"] : keys),
            where: field.equals(parsedValues[field.key]),
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
    const parsedValues: any = values.map((value) => (table.schema as zod.ZodObject<any>).partial().parse(value));
    const adapator = this.options.adaptor;
    return toLazyPromise(
      async (): Promise<SqlResult<void, 0>> => {
        if (parsedValues.length === 0) {
          return {
            results: [],
            first: undefined,
            timings: {
              wallTimeMs: 0,
            },
          };
        }
        await adapator.executeUpdateMany(table, parsedValues, field);
        return {
          results: [],
          first: undefined,
        };
      },
      {
        async selectMutated<TKey extends BindingKeys<TTable>>(
          ...keys: TKey[]
        ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, number>> {
          if (parsedValues.length === 0) {
            return {
              results: [],
              first: undefined,
            } as any;
          }
          const result = await adapator.executeUpdateMany(table, parsedValues, field);
          if (result.results.length > 0) {
            return result as any;
          }

          return adapator.executeSelect({
            table,
            fields: getFieldBindingsByKeys(table, keys),
            where: field.in(parsedValues.map((value: any) => value[field.key])),
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

    const parsedValues = values.map((v) => table.schema.parse(v));
    return this.options.adaptor.executeInsertMany(table, parsedValues as any);
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
      const field = Object.entries(table.fields).find(([key]) => key === column.name)?.[1];
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
  mapper: (from: TFrom) => TTo | undefined,
): SqlResult<TTo, TResultLimit> => {
  const mapped = (result.results as any).reduce((acc: TTo[], result: any) => {
    const mapped = mapper(result);
    if (mapped !== undefined) {
      acc.push(mapped);
    }
    return acc;
  }, []);

  return {
    ...result,
    first: mapped.length > 0 ? mapped[0] : undefined,
    results: mapped,
  } as SqlResult<TTo, TResultLimit>;
};

export { meta } from "zod-meta";
export type { default as DatabaseAdaptor } from "./DatabaseAdaptor";
export * from "./MetaTypes";
export type { SelectCondition, SelectQueryBuilder } from "./QueryBuilder";
