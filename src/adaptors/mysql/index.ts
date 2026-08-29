import type { Connection, Pool, RowDataPacket } from "mysql2/promise";
import * as zod from "zod/v4";
import { getMetaItem, type ZodMetaItem } from "zod-meta";
import DatabaseAdaptor, { type PossiblySelectedResult } from "../../DatabaseAdaptor";
import { quoteMysqlIdentifier } from "../../Escaping";
import {
  type BackfillOptions,
  backfill,
  type ExecuteStatementEvent,
  type FieldDiffType,
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
  valueToSql,
} from "../../index";
import {
  buildConditionSql,
  type InputOfTable,
  type SelectCondition,
  type SelectQuery,
  type SingleFieldBinding,
  type SqlResult,
  type SqlResultTimings,
  type StringKeys,
  type ValueOfTable,
} from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";

const JSON_START = /[{[]/;
const JSON_END = /[\]}]/;

type BackfillMetaItem = ZodMetaItem<BackfillOptions>;

const getRequiredBackfillMeta = (schema: zod.Schema<any>): BackfillMetaItem | undefined => {
  if (isZodRequired(schema)) {
    return getMetaItem(schema, backfill);
  }
  return undefined;
};

const TYPE_ORDERING: Record<FieldDiffType, number> = {
  modified: 0,
  added: 1,
  removed: 2,
};

export default class MysqlAdaptor<
  TDriver extends Connection | Pool = Connection | Pool,
> extends DatabaseAdaptor<TDriver> {
  quoteIdentifier(value: string): string {
    return quoteMysqlIdentifier(value);
  }

  typeToSql(type: zod.ZodType<any>): string {
    if (isZodTypeExtends(type, zod.ZodObject) || isZodTypeExtends(type, zod.ZodArray)) {
      return "JSON";
    }
    if (isZodTypeExtends(type, zod.ZodNumber) && super.typeToSql(type) === "REAL") {
      return "DOUBLE";
    }
    if (isZodTypeExtends(type, zod.ZodDate)) {
      return "DATETIME";
    }
    if (isZodTypeExtends(type, zod.ZodString)) {
      return "VARCHAR(255)";
    }
    return super.typeToSql(type);
  }

  buildJsonArrayContainsSql(fieldSql: string, value: unknown): Statement {
    return sql`JSON_CONTAINS(${raw(fieldSql)}, JSON_ARRAY(${value}))`;
  }

  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const startTimestamp = Date.now();
    const rawSql = statement[TO_SQL_SYMBOL]();
    let success = false;
    let timings: SqlResultTimings | undefined;

    try {
      const [queryResult] = await this.driver.query(rawSql);
      const rows = Array.isArray(queryResult) ? (queryResult as RowDataPacket[]) : [];
      success = true;
      timings = { wallTimeMs: Date.now() - startTimestamp };
      return this.mapResult({ results: rows, first: rows[0], timings });
    } finally {
      const event: ExecuteStatementEvent = {
        sql: rawSql,
        timings: timings ?? { wallTimeMs: Date.now() - startTimestamp },
        success,
      };
      this.options.events?.onExecuteStatement?.(event);
    }
  }

  protected mapResult(value: SqlResult): SqlResult {
    return mapSqlResult(value, (row) =>
      Object.fromEntries(
        Object.entries(row).map(([key, fieldValue]) => {
          if (
            typeof fieldValue === "string" &&
            JSON_START.test(fieldValue[0]) &&
            JSON_END.test(fieldValue[fieldValue.length - 1])
          ) {
            try {
              return [key, JSON.parse(fieldValue)];
            } catch {}
          }
          return [key, fieldValue];
        }),
      ),
    );
  }

  buildSelectSql(select: SelectQuery): Statement {
    const tableName = this.quoteIdentifier(String(select.table.id));
    const offsetSql = select.offset === undefined ? "" : ` OFFSET ${select.offset}`;
    const limitSql =
      select.limit !== undefined
        ? ` LIMIT ${select.limit}`
        : select.offset !== undefined
          ? " LIMIT 18446744073709551615"
          : "";

    return sql`SELECT ${raw(
      select.fields.map((field) => (field.key === "*" ? "*" : this.quoteIdentifier(String(field.key)))),
    )}
      FROM ${raw(tableName)}${select.where ? sql` WHERE ${buildConditionSql(this, select.where)}` : raw("")}${
        select.orderBy.length > 0
          ? sql` ORDER BY ${raw(
              select.orderBy.map((order) => `${this.quoteIdentifier(String(order.field.key))} ${order.direction}`),
            )}`
          : raw("")
      }${raw(limitSql)}${raw(offsetSql)}`;
  }

  executeSelect<R>(select: SelectQuery): Promise<R> {
    return this.execute(this.buildSelectSql(select)) as Promise<R>;
  }

  async executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>, 1>> {
    const statement = sql`INSERT INTO ${raw(this.quoteIdentifier(String(table.id)))} (${raw(
      Object.keys(values as object).map((key) => this.quoteIdentifier(key)),
    )}) VALUES (${raw(Object.values(values as object).map((value) => valueToSql(value, true)))})`;
    await this.execute(statement);
    return shouldReturn
      ? { first: values as ValueOfTable<TTable>, results: [values as ValueOfTable<TTable>] }
      : { first: undefined, results: [] };
  }

  async executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>, number>> {
    const fieldKeys = Object.keys(table.fields);
    const statement = sql`INSERT INTO ${raw(this.quoteIdentifier(String(table.id)))} (${raw(
      fieldKeys.map((key) => this.quoteIdentifier(key)),
    )}) VALUES ${raw(
      values.map(
        (value) => `(${fieldKeys.map((key) => valueToSql((value as Record<string, unknown>)[key], true)).join(", ")})`,
      ),
    )}`;
    await this.execute(statement);
    return shouldReturn
      ? { first: values[0] as ValueOfTable<TTable>, results: values as ValueOfTable<TTable>[] }
      : { first: undefined, results: [] };
  }

  async fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>> {
    const result = await this.execute(sql`
      SELECT
        c.column_name AS column_name,
        c.is_nullable AS is_nullable,
        c.column_default AS column_default,
        c.extra AS extra,
        CASE WHEN c.column_key = 'PRI' THEN 1 ELSE 0 END AS is_primary_key
      FROM information_schema.columns c
      WHERE c.table_schema = DATABASE()
        AND c.table_name = ${String(table.id)}
      ORDER BY c.ordinal_position
    `);

    return mapSqlResult<any, TableColumnInfo, number>(result, (row) => ({
      name: row.column_name,
      type: {} as any,
      notNull: row.is_nullable === "NO",
      hasDefault: row.column_default !== null,
      isIdentity: String(row.extra).includes("auto_increment"),
      primaryKey: Boolean(row.is_primary_key),
    }));
  }

  async executeUpdate<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<PossiblySelectedResult<ValueOfTable<TTable>>> {
    const result = (await this.execute(this.buildUpdateSql(table, values, where))) as PossiblySelectedResult<
      ValueOfTable<TTable>
    >;
    result.selected = false;
    return result;
  }

  async executeUpsert<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    _field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Promise<SqlResult<void, 0>> {
    const keys = Object.keys(values);
    const statement = sql`INSERT INTO ${raw(this.quoteIdentifier(String(table.id)))} (${raw(
      keys.map((key) => this.quoteIdentifier(key)),
    )}) VALUES (${raw(Object.values(values).map((value) => valueToSql(value, true)))})
      ON DUPLICATE KEY UPDATE ${raw(
        keys
          .map((key) => `${this.quoteIdentifier(key)} = ${valueToSql((values as Record<string, unknown>)[key], true)}`)
          .join(", "),
      )}`;
    return this.execute(statement) as Promise<SqlResult<void, 0>>;
  }

  async executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
    where: SelectCondition<ValueOfTable<TTable>> | undefined,
  ): Promise<SqlResult<Record<TKey, number>, 1>> {
    const statement = sql`SELECT ${raw(
      fields.map((field) => {
        const key = field.key === "*" ? "_count" : String(field.key);
        const expression = field.key === "*" ? "*" : `ALL ${this.quoteIdentifier(String(field.key))}`;
        return `COUNT(${expression}) AS ${this.quoteIdentifier(key)}`;
      }),
    )} FROM ${raw(this.quoteIdentifier(String(table.id)))}${
      where ? sql` WHERE ${buildConditionSql(this, where)}` : raw("")
    }`;
    const result = await this.execute(statement);
    return mapSqlResult(result, (row: Record<string, string | number>) =>
      Object.fromEntries(Object.entries(row).map(([key, value]) => [key, Number(value)])),
    ) as SqlResult<Record<TKey, number>, 1>;
  }

  executeDelete<TTable extends Table>(
    table: TTable,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>> {
    return this.execute(
      sql`DELETE FROM ${raw(this.quoteIdentifier(String(table.id)))} WHERE ${buildConditionSql(this, where)}`,
    ) as Promise<SqlResult<void, 0>>;
  }

  protected buildUpdateSql<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Statement {
    return sql`UPDATE ${raw(this.quoteIdentifier(String(table.id)))} SET ${raw(
      Object.entries(values)
        .filter(([, value]) => value !== undefined)
        .map(([key, value]) => `${this.quoteIdentifier(key)} = ${valueToSql(value, true)}`)
        .join(", "),
    )} WHERE ${buildConditionSql(this, where)}`;
  }

  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>> {
    const startTimestamp = Date.now();
    await Promise.all(
      values.map((value) =>
        this.execute(this.buildUpdateSql(table, value, field.equals(value[field.key] as any) as any)),
      ),
    );
    return { results: [], first: undefined, timings: { wallTimeMs: Date.now() - startTimestamp } };
  }

  async processDiff(table: Table, diff: TableDiff): Promise<void> {
    let tableHasRows: boolean | undefined;
    for (const fieldDiff of diff.fields) {
      const schema = fieldDiff.field?.schema;
      const addsRequiredField = fieldDiff.type === "added" && schema && isZodRequired(schema);
      const addsRequiredConstraint =
        fieldDiff.type === "modified" &&
        fieldDiff.modifications?.some(
          (modification) => modification.type === "add-constraint" && modification.constraint === "NOT NULL",
        );
      let requiresBackfill = false;
      if (addsRequiredField) {
        tableHasRows ??=
          (await this.execute(sql`SELECT 1 FROM ${raw(this.quoteIdentifier(String(table.id)))} LIMIT 1`)).results
            .length > 0;
        requiresBackfill = tableHasRows;
      } else if (addsRequiredConstraint) {
        requiresBackfill =
          (
            await this.execute(
              sql`SELECT 1 FROM ${raw(this.quoteIdentifier(String(table.id)))}
                  WHERE ${raw(this.quoteIdentifier(String(fieldDiff.key)))} IS NULL LIMIT 1`,
            )
          ).results.length > 0;
      }
      if (schema && requiresBackfill) {
        const backfillMeta = getMetaItem(schema, backfill) as BackfillMetaItem | undefined;
        if (backfillMeta?.data.value === undefined || backfillMeta.data.value === null) {
          throw new Error(`[zodbase] Backfill value is required when adding required field '${fieldDiff.field?.key}'`);
        }
      }
    }

    diff.fields.sort((left, right) => TYPE_ORDERING[left.type] - TYPE_ORDERING[right.type]);
    for (const fieldDiff of diff.fields) {
      const field = fieldDiff.field;
      const schema = field?.schema;
      const tableSql = raw(this.quoteIdentifier(String(table.id)));
      const columnSql = raw(this.quoteIdentifier(String(fieldDiff.key)));

      if (fieldDiff.type === "added" && schema) {
        await this.execute(sql`ALTER TABLE ${tableSql} ADD COLUMN ${columnSql} ${raw(this.typeToSql(schema))} NULL`);
        const backfillMeta = getRequiredBackfillMeta(schema);
        if (backfillMeta) {
          const backfillValue = backfillMeta.data.value;
          if (backfillValue === undefined || backfillValue === null) {
            throw new Error(`[zodbase] Backfill value is required when adding required field '${field?.key}'`);
          }
          await this.execute(sql`UPDATE ${tableSql} SET ${columnSql} = ${backfillValue} WHERE ${columnSql} IS NULL`);
        }
        if (isZodRequired(schema)) {
          await this.execute(
            sql`ALTER TABLE ${tableSql} MODIFY COLUMN ${columnSql} ${raw(this.typeToSql(schema))} NOT NULL`,
          );
        }
        if (getMetaItem(schema, primaryKey)) {
          await this.execute(sql`ALTER TABLE ${tableSql} ADD PRIMARY KEY (${columnSql})`);
        }
      } else if (fieldDiff.type === "removed") {
        await this.execute(sql`ALTER TABLE ${tableSql} DROP COLUMN ${columnSql}`);
      } else if (fieldDiff.type === "modified" && schema) {
        const backfillMeta = getRequiredBackfillMeta(schema);
        if (backfillMeta) {
          const backfillValue = backfillMeta.data.value;
          if (backfillValue === undefined || backfillValue === null) {
            throw new Error(`[zodbase] Backfill value is required when adding required field '${field?.key}'`);
          }
          await this.execute(sql`UPDATE ${tableSql} SET ${columnSql} = ${backfillValue} WHERE ${columnSql} IS NULL`);
        }
        for (const modification of fieldDiff.modifications ?? []) {
          if (modification.constraint === "NOT NULL") {
            await this.execute(
              sql`ALTER TABLE ${tableSql} MODIFY COLUMN ${columnSql} ${raw(this.typeToSql(schema))} ${raw(
                modification.type === "add-constraint" ? "NOT NULL" : "NULL",
              )}`,
            );
          }
        }
      }
    }
  }

  async syncTableIndexes(table: Table): Promise<void> {
    for (const index of table.indexes) {
      if (index.where) {
        throw new Error("MySQL and MariaDB do not support partial indexes");
      }
      const existing = await this.execute(sql`
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = ${String(table.id)}
          AND index_name = ${index.id}
        LIMIT 1
      `);
      if (existing.results.length === 0) {
        await this.execute(sql`
          CREATE ${raw(index.unique ? "UNIQUE " : "")}INDEX ${raw(this.quoteIdentifier(index.id))}
          ON ${raw(this.quoteIdentifier(String(table.id)))} (${raw(
            index.fields.map((field) => this.quoteIdentifier(String(field.key))).join(", "),
          )})
        `);
      }
    }
  }

  createTable(table: Table, name?: string): Promise<SqlResult> {
    const statement = sql`CREATE TABLE IF NOT EXISTS ${raw(this.quoteIdentifier(name ?? String(table.id)))} (
      ${join(
        Object.values(table.fields).map((field) => {
          const primaryKeyMeta = getMetaItem(field.schema, primaryKey);
          const typeSql = this.typeToSql(field.schema);
          return raw(
            [
              this.quoteIdentifier(String(field.key)),
              typeSql,
              primaryKeyMeta ? "PRIMARY KEY" : "",
              isZodRequired(field.schema) ? "NOT NULL" : "",
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
