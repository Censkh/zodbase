import type * as zod from "zod";
import type { ZodType } from "zod";
import { getMetaStore } from "zod-meta";
import { createTableBinding } from "./Bindings";
import type { SingleFieldBinding, StringKeys } from "./QueryBuilder";
import { TO_SQL_SYMBOL, type ToSql } from "./Statement";
import type { TypeToken } from "./TypeToken";

export interface TableColumnInfo {
  name: string;
  type: ZodType;
  notNull: boolean;
  primaryKey: boolean;
}

export type Bindings<TValue extends zod.ZodRawShape> = {
  [K in keyof TValue]-?: K extends StringKeys<TValue> ? SingleFieldBinding<TValue, K> : never;
};

export type PrefixKeys<T, P extends string> = {
  [K in keyof T as `${P}${K & string}`]: T[K];
};

export type Table<
  TValue extends zod.infer<TSchema> = any,
  TName extends string = string,
  TSchema extends zod.ZodObject<any> = zod.ZodObject<any>,
> = PrefixKeys<Bindings<TValue>, "$"> &
  ToSql &
  Omit<TableOptions<TValue, TName, TSchema>, "id"> & {
    id: TName & ToSql;
    fields: Bindings<TValue>;
  };

export interface TableOptions<
  TValue extends zod.infer<TSchema>,
  TName extends string,
  TSchema extends zod.ZodObject<any>,
> {
  id: TName;
  as?: TypeToken<TValue>;
  schema: TSchema;
}

export const createTable = <
  TValue extends zod.infer<TSchema>,
  TName extends string,
  TSchema extends zod.ZodObject<any>,
>(
  options: TableOptions<TValue, TName, TSchema>,
): Table<TValue, TName, TSchema> => {
  for (const [key, value] of Object.entries(options.schema._def.shape())) {
    const fieldSchema = value as zod.ZodType;
    const metaStore = getMetaStore(fieldSchema);
    if (!metaStore) {
      continue;
    }
    for (const metaItem of metaStore.itemList) {
      if (metaItem?.type.check) {
        const valid = metaItem.type.check(fieldSchema, metaItem.data);
        if (valid.success === false) {
          console.error(
            `[zodbase] Invalid meta '${metaItem.type.id}' for field '${key}': ${valid.message}`,
          );
          //return undefined;
        }
      }
    }
  }

  const table = {
    ...options,
    fields: {},
    id: Object.assign(options.id, {
      [TO_SQL_SYMBOL]: () => options.id,
    }),
    [TO_SQL_SYMBOL]: () => options.id,
  } as any as Table<TValue, TName, TSchema>;
  table.fields = createTableBinding(table) as any;
  for (const [key, field] of Object.entries(table.fields)) {
    // @ts-ignore
    table[`$${key}`] = field;
  }
  return table;
};
