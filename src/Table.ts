import type * as zod from "zod";
import { getMetaItem, getMetaStores, getZodTypeFields } from "zod-meta";
import { createTableBinding } from "./Bindings";
import { quoteIdentifier } from "./Escaping";
import { primaryKey as primaryKeyMeta } from "./MetaTypes";
import type { SelectCondition, SingleFieldBinding, StringKeys } from "./QueryBuilder";
import { TO_SQL_SYMBOL, type ToSql } from "./Statement";
import type { BaseSchema } from "./Types";
import type { TypeToken } from "./TypeToken";

export interface TableColumnInfo {
  name: string;
  type: BaseSchema;
  sqlType?: string;
  notNull: boolean;
  primaryKey: boolean;
  foreignKey?: TableForeignKeyInfo;
}

/**
 * A table as the database holds it: its columns and its secondary indexes and unique constraints,
 * leaving out primary keys and what the engine keeps for itself (hidden columns, autoindexes).
 */
export interface SchemaTableInfo {
  name: string;
  columns: string[];
  indexes: SchemaIndexInfo[];
}

export interface SchemaIndexInfo {
  name: string;
  unique: boolean;
}

/** What a database holds that the given table definitions don't define. */
export interface SchemaDrift {
  /** Tables no definition names, with how many rows each holds. */
  tables: { name: string; rowCount: number }[];
  /** Columns of defined tables that their definition doesn't name. */
  columns: { table: string; column: string }[];
  /** Indexes and unique constraints of defined tables that their definition doesn't name. */
  indexes: { table: string; index: string; unique: boolean }[];
}

export interface TableForeignKeyInfo {
  table: string;
  field: string;
  onDelete: import("./MetaTypes").ForeignKeyAction;
  constraintName?: string;
}

export type Bindings<TValue, TName extends string = string> = {
  [K in keyof TValue]-?: K extends StringKeys<TValue> ? SingleFieldBinding<TValue, K, TName> : never;
};

export type PrefixKeys<T, P extends string> = {
  [K in keyof T as `${P}${K & string}`]: T[K];
};

export type Table<
  TValue extends zod.infer<TSchema> = any,
  TName extends string = string,
  TSchema extends BaseSchema = BaseSchema,
> = PrefixKeys<Bindings<TValue, TName>, "$"> &
  ToSql &
  Omit<TableOptions<TValue, TName, TSchema>, "id"> & {
    id: TName & ToSql;
    sourceTable?: Table;
    fields: Bindings<TValue, TName>;
    indexes: TableIndex[];
    addIndex(id: string, fields: SingleFieldBinding[], options?: TableIndexOptions): Table<TValue, TName, TSchema>;
  };

export interface TableIndexOptions {
  unique?: boolean;
  where?: SelectCondition;
}

export interface TableIndex extends TableIndexOptions {
  id: string;
  fields: SingleFieldBinding[];
}

export interface TableOptions<TValue extends zod.infer<TSchema>, TName extends string, TSchema extends BaseSchema> {
  id: TName;
  as?: TypeToken<TValue>;
  schema: TSchema;
  /**
   * The primary key's columns in order, for a key of more than one column. A one-column key can
   * instead mark its field with `primaryKey()`; with this option, a field marked so must be one of
   * these.
   */
  primaryKey?: readonly StringKeys<TValue>[];
}

/** The table's primary key columns in order: empty when it has none. */
export const getPrimaryKey = (table: Pick<Table, "primaryKey" | "fields">): string[] =>
  table.primaryKey
    ? table.primaryKey.map(String)
    : Object.values(table.fields as Record<string, SingleFieldBinding>)
        .filter((field) => getMetaItem(field.schema, primaryKeyMeta))
        .map((field) => String(field.key));

export const createTable = <TValue extends zod.infer<TSchema>, TName extends string, TSchema extends BaseSchema>(
  options: TableOptions<TValue, TName, TSchema>,
): Table<TValue, TName, TSchema> => {
  const fields = getZodTypeFields(options.schema);
  for (const field of fields) {
    const fieldSchema = field.schema;
    const metaStores = getMetaStores(fieldSchema);
    for (const metaStore of metaStores) {
      for (const metaItem of metaStore.itemList) {
        if (metaItem?.type.check) {
          const valid = metaItem.type.check(fieldSchema, metaItem.data);
          if (valid.success === false) {
            console.error(`[zodbase] Invalid meta '${metaItem.type.id}' for field '${field.key}': ${valid.message}`);
            //return undefined;
          }
        }
      }
    }
  }

  const marked = fields.filter((field) => getMetaItem(field.schema, primaryKeyMeta)).map((field) => String(field.key));
  if (options.primaryKey) {
    const keys = options.primaryKey.map(String);
    const known = new Set(fields.map((field) => String(field.key)));
    const unknown = keys.find((key) => !known.has(key));
    if (unknown)
      throw new Error(`[zodbase] Primary key column '${unknown}' of '${options.id}' is not one of its fields`);
    if (new Set(keys).size !== keys.length)
      throw new Error(`[zodbase] Primary key of '${options.id}' names a column more than once`);
    const outside = marked.find((key) => !keys.includes(key));
    if (outside)
      throw new Error(`[zodbase] '${outside}' of '${options.id}' is marked primaryKey() but isn't in its primaryKey`);
  } else if (marked.length > 1) {
    throw new Error(
      `[zodbase] '${options.id}' marks ${marked.join(", ")} as primaryKey(): give a key of more than one column in order with the table's primaryKey option`,
    );
  }

  const table = {
    ...options,
    fields: {},
    indexes: [],
    id: Object.assign(options.id, {
      [TO_SQL_SYMBOL]: () => quoteIdentifier(options.id),
    }),
    [TO_SQL_SYMBOL]: () => quoteIdentifier(options.id),
    addIndex(id: string, fields: SingleFieldBinding[], indexOptions = {}) {
      this.indexes.push({
        id,
        fields,
        ...indexOptions,
      });
      return this;
    },
  } as any as Table<TValue, TName, TSchema>;
  table.fields = createTableBinding(table) as any;
  for (const [key, field] of Object.entries(table.fields)) {
    // @ts-expect-error
    table[`$${key}`] = field;
  }
  return table;
};

/** A distinct SQL scope for self joins and correlated subqueries. */
export const alias = <T extends Table, N extends string>(
  table: T,
  name: N,
): Table<zod.infer<T["schema"]>, N, T["schema"]> => {
  const aliased = createTable({ id: name, schema: table.schema });
  aliased.sourceTable = table.sourceTable ?? table;
  return aliased as Table<zod.infer<T["schema"]>, N, T["schema"]>;
};
