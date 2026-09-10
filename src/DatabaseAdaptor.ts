import * as zod from "zod";
import { getMetaItem } from "zod-meta";
import { quoteIdentifier } from "./Escaping";
import {
  type DatabaseEvents,
  foreignKey,
  isZodRequired,
  isZodTypeExtends,
  join,
  mapSqlResult,
  primaryKey,
  raw,
  sql,
  type Table,
  type TableColumnInfo,
  type TableDiff,
} from "./index";
import type {
  InputOfTable,
  SelectCondition,
  SelectQuery,
  SingleFieldBinding,
  SqlResult,
  StringKeys,
  ValueOfTable,
} from "./QueryBuilder";
import type { Statement } from "./Statement";

export interface DatabaseAdaptorOptions<TDriver = any> {
  driver: TDriver;
  debug?: boolean;
  events?: DatabaseEvents;
}

export interface PossiblySelectedResult<TValue = any, TLimit extends number = number>
  extends SqlResult<TValue, TLimit> {
  selected: boolean;
}

export default abstract class DatabaseAdaptor<TDriver = any> {
  private transactionQueue: Promise<void> = Promise.resolve();

  constructor(protected readonly options: DatabaseAdaptorOptions<TDriver>) {}

  protected get driver() {
    return this.options.driver;
  }

  quoteIdentifier(value: string): string {
    return quoteIdentifier(value);
  }

  protected selectFields(table: Table, fields?: SelectQuery["fields"]): string {
    const expanded = fields
      ? fields.flatMap((field) => (field.key === "*" ? Object.values(table.fields) : [field]))
      : Object.values(table.fields);
    const needsCast = (field: SelectQuery["fields"][number]) =>
      isZodTypeExtends(field.schema, zod.ZodBigInt) || isZodTypeExtends(field.schema, zod.ZodDate);
    if (!expanded.some(needsCast)) {
      return fields
        ? fields.map((field) => (field.key === "*" ? "*" : this.quoteIdentifier(String(field.key)))).join(", ")
        : "*";
    }
    return expanded
      .map((field) => {
        const column = this.quoteIdentifier(String(field.key));
        // Read exact integers and UTC date text before a driver can coerce them lossily.
        return needsCast(field) ? `CAST(${column} AS ${this.textCastType}) AS ${column}` : column;
      })
      .join(", ");
  }

  protected textCastType = "TEXT";

  protected decodeResult(table: Table, result: SqlResult): SqlResult {
    return mapSqlResult(result, (row) =>
      Object.fromEntries(
        Object.entries(row).map(([key, value]) => {
          const schema = table.fields[key]?.schema;
          if (!schema || value == null) return [key, value];
          if (isZodTypeExtends(schema, zod.ZodBigInt)) return [key, BigInt(value as string)];
          if (isZodTypeExtends(schema, zod.ZodBoolean))
            return [key, value === true || value === 1 || value === BigInt(1)];
          if (isZodTypeExtends(schema, zod.ZodDate)) {
            if (value instanceof Date) return [key, value];
            const text = String(value).replace(" ", "T");
            return [key, new Date(/[zZ]|[+-]\d\d(?::?\d\d)?$/.test(text) ? text : `${text}Z`)];
          }
          if (
            typeof value === "string" &&
            (isZodTypeExtends(schema, zod.ZodObject) ||
              isZodTypeExtends(schema, zod.ZodArray) ||
              isZodTypeExtends(schema, zod.ZodRecord))
          ) {
            return [key, JSON.parse(value)];
          }
          // Untyped legacy columns may contain serialized structured values.
          if (
            typeof value === "string" &&
            (isZodTypeExtends(schema, zod.ZodAny) || isZodTypeExtends(schema, zod.ZodUnknown)) &&
            /^[{[]/.test(value)
          ) {
            try {
              return [key, JSON.parse(value)];
            } catch {}
          }
          return [key, value];
        }),
      ),
    );
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
    shouldReturn?: boolean,
  ): Promise<SqlResult<ValueOfTable<TTable>, 1>>;
  abstract executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
    shouldReturn?: boolean,
  ): Promise<SqlResult<ValueOfTable<TTable>, number>>;
  abstract executeUpdate<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
    shouldReturn?: boolean,
  ): Promise<PossiblySelectedResult<ValueOfTable<TTable>>>;
  abstract executeUpsert<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Promise<SqlResult<void, 0>>;
  abstract executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>>;
  abstract executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
    where: SelectCondition<ValueOfTable<TTable>> | undefined,
  ): Promise<SqlResult<Record<TKey, number>, 1>>;
  abstract executeDelete<TTable extends Table>(
    table: TTable,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>>;

  abstract buildJsonArrayContainsSql(fieldSql: string, value: unknown): Statement;

  abstract fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>>;
  abstract syncTableIndexes(table: Table): Promise<void>;

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
      const definition = numberType.def as any;
      const isInt =
        definition.format === "safeint" ||
        definition.checks?.some(
          (check: any) => check.isInt || check.format === "safeint" || check.def?.format === "safeint",
        );
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

  transaction<TResult>(callback: (adaptor: DatabaseAdaptor) => Promise<TResult>): Promise<TResult> {
    const transaction = this.transactionQueue.then(() => this.executeTransaction(callback));
    this.transactionQueue = transaction.then(
      () => undefined,
      () => undefined,
    );
    return transaction;
  }

  protected async executeTransaction<TResult>(
    callback: (adaptor: DatabaseAdaptor) => Promise<TResult>,
  ): Promise<TResult> {
    await this.execute(raw("BEGIN"));
    try {
      const result = await callback(this);
      await this.execute(raw("COMMIT"));
      return result;
    } catch (error) {
      await this.execute(raw("ROLLBACK")).catch(() => undefined);
      throw error;
    }
  }

  createTable(table: Table, name?: string) {
    const statement = sql`CREATE TABLE IF NOT EXISTS ${raw(quoteIdentifier(name ?? String(table.id)))}
      (
        ${join(
          Object.values(table.fields).map((field) => {
            const schema = field.schema;
            const primaryKeyMeta = getMetaItem(schema, primaryKey);
            const foreignKeyMeta = getMetaItem(schema, foreignKey);
            //const autoIncrementMeta = getMetaItem(schema, autoIncrement);
            return raw(
              [
                quoteIdentifier(String(field.key)),
                this.typeToSql(schema),
                primaryKeyMeta ? "PRIMARY KEY" : "",
                isZodRequired(schema) ? " NOT NULL" : "",
                foreignKeyMeta
                  ? `REFERENCES ${quoteIdentifier(String(foreignKeyMeta.data.field.table.id))} (${quoteIdentifier(String(foreignKeyMeta.data.field.key))}) ON DELETE ${(foreignKeyMeta.data.onDelete ?? "no action").toUpperCase()}`
                  : "",
              ]
                .filter(Boolean)
                .join(" "),
            );
          }),
          ", ",
        )}
      )`;

    return this.execute(statement);
  }
}
