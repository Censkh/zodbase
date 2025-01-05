// @ts-ignore
import * as zod from "zod";
import { getMetaItem } from "zod-meta";
import type {
  InputOfTable,
  SelectCondition,
  SelectQuery,
  SingleFieldBinding,
  SqlDefiniteResult,
  SqlResult,
  StringKeys,
  ValueOfTable,
} from "./QueryBuilder";
import type { Statement } from "./Statement";
import {
  type Table,
  type TableColumnInfo,
  type TableDiff,
  isZodRequired,
  isZodTypeExtends,
  join,
  primaryKey,
  raw,
  sql,
} from "./index";

export default abstract class DatabaseAdaptor<TDriver = any> {
  protected readonly driver: TDriver;

  constructor(driver: TDriver) {
    this.driver = driver;
  }

  //typeToSql: (type: zod.ZodType<any>) => string;
  //valuesSql: <T extends zod.ZodSchema>(values: zod.infer<T>, schema: T) => string;
  abstract executeSelect<TTable extends Table, TLimit extends number>(
    select: SelectQuery<Table, TLimit>,
  ): Promise<SqlResult<ValueOfTable<TTable>, TLimit>>;
  abstract execute(statement: Statement): Promise<SqlResult>;
  abstract executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>>;
  abstract executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, number>>;
  abstract executeUpdate<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>>;
  abstract executeUpsert<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Promise<SqlResult<void, 0>>;
  abstract executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(
    table: TTable,
    values: TValue[],
    field: SingleFieldBinding<TValue, TKey>,
  ): Promise<SqlResult<void, 0>>;
  abstract executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
    where: SelectCondition<ValueOfTable<TTable>> | undefined,
  ): Promise<SqlResult<Record<TKey, number>, 1>>;
  abstract executeDelete<TTable extends Table>(
    table: TTable,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>>;

  abstract fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>>;

  typeToSql(type: zod.ZodType<any>): string {
    if (isZodTypeExtends(type, zod.ZodObject)) {
      return "JSONB";
    }
    if (isZodTypeExtends(type, zod.ZodArray)) {
      return "JSONB";
    }
    if (isZodTypeExtends(type, zod.ZodNull)) {
      return "NULL";
    }
    if (isZodTypeExtends(type, zod.ZodString)) {
      return "TEXT";
    }
    const numberType = isZodTypeExtends(type, zod.ZodNumber);
    if (numberType) {
      // @ts-ignore
      const isInt = numberType._def.checks?.find((check) => check.kind === "int");
      return isInt ? "INTEGER" : "REAL";
    }
    if (isZodTypeExtends(type, zod.ZodBoolean)) {
      return "BOOLEAN";
    }
    if (isZodTypeExtends(type, zod.ZodDate)) {
      return "TIMESTAMP";
    }
    if (isZodTypeExtends(type, zod.ZodBigInt)) {
      return "BIGINT";
    }
    if (isZodTypeExtends(type, zod.ZodUndefined)) {
      return "NULL";
    }
    return "TEXT";
  }

  abstract processDiff(table: Table, diff: TableDiff): Promise<void>;

  createTable(table: Table, name?: string) {
    const statement = sql`CREATE TABLE IF NOT EXISTS ${name ?? table.id}
      (
        ${join(
          Object.entries(table.schema.shape).map(([key, value]) => {
            const schema = value as zod.ZodType;
            const primaryKeyMeta = getMetaItem(schema, primaryKey);
            return sql`  ${raw(key)} ${raw(this.typeToSql(schema))}${primaryKeyMeta ? raw(" PRIMARY KEY") : ""}${
              isZodRequired(schema) ? raw(" NOT NULL") : ""
            }`;
          }),
          ",",
        )}
      )`;

    return this.execute(statement);
  }
}