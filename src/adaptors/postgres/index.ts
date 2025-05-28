import type * as pg from "pg";
import type * as zod from "zod/v4";
import { getMetaItem, type ZodMetaItem } from "zod-meta";
import DatabaseAdaptor from "../../DatabaseAdaptor";
import {
  type BackfillOptions,
  backfill,
  type ExecuteStatementEvent,
  type FieldDiffType,
  isZodRequired,
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
  type SqlDefiniteResult,
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

export default class PostgresAdaptor<TDriver extends pg.Client> extends DatabaseAdaptor<TDriver> {
  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    const startTimestamp = Date.now();
    const rawSql = statement[TO_SQL_SYMBOL]();
    let success = false;
    let timings: SqlResultTimings | undefined;

    try {
      const res = await this.driver.query(rawSql);
      success = true;

      timings = {
        wallTimeMs: Date.now() - startTimestamp,
      };

      return this.mapResult({
        results: res.rows,
        first: res.rows[0],
        timings: timings,
      });
    } finally {
      const event: ExecuteStatementEvent = {
        sql: rawSql,
        timings: timings ?? {
          wallTimeMs: Date.now() - startTimestamp,
        },
        success: success,
      };
      this.options.events?.onExecuteStatement?.(event);

      if (this.options.debug) {
        console.debug("PostgresAdaptor.execute", "Executed SQL", event);
      }
    }
  }

  protected mapResult(value: SqlResult): SqlResult {
    return mapSqlResult(value, (value) => {
      return Object.fromEntries(
        Object.entries(value).map(([key, value]) => {
          if (typeof value === "string" && JSON_START.test(value[0]) && JSON_END.test(value[value.length - 1])) {
            try {
              const parsedValue = JSON.parse(value);
              return [key, parsedValue];
            } catch {}
          }
          return [key, value];
        }),
      );
    });
  }

  buildSelectSql(select: SelectQuery): Statement {
    return sql`SELECT ${raw(select.fields.map((field) => (field.key === "*" ? "*" : `"${field.key}"`)))}
               FROM ${select.table} ${
                 select.where
                   ? sql` WHERE
               ${buildConditionSql(this, select.where, true)}`
                   : raw("")
               }${
                 select.orderBy.length > 0
                   ? sql` ORDER BY
                   ${raw(select.orderBy.map((order) => `"${order.field.key}" ${order.direction}`))}`
                   : raw("")
               }${raw(select.limit ? ` LIMIT ${select.limit}` : "")}${raw(select.offset ? ` OFFSET ${select.offset}` : "")}`;
  }

  async executeSelect<R>(select: SelectQuery): Promise<R> {
    const sql = this.buildSelectSql(select);
    return this.execute(sql) as any;
  }

  async executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
    const parsedValues = table.schema.parse(values) as any;
    const statement = sql`INSERT INTO ${table.id} (${raw(Object.keys(parsedValues).map((k) => `"${k}"`))})
                          VALUES (${raw(Object.values(parsedValues).map((v) => valueToSql(v, true)))}) RETURNING *`;
    return (await this.execute(statement)) as any;
  }

  async executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, number>> {
    const parsedValues = values.map((value) => table.schema.parse(value)) as any;
    const statement = sql`INSERT INTO ${table.id} (${raw(Object.keys(parsedValues[0]).map((k) => `"${k}"`))})
                          VALUES ${raw(
                            parsedValues.map(
                              (value: any) => sql`(${raw(Object.values(value).map((v) => valueToSql(v, true)))})`,
                            ),
                           )} RETURNING *`;
    return (await this.execute(statement)) as any;
  }

  async fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>> {
    const columnResult = await this.execute(sql`
      SELECT 
        c.column_name,
        c.is_nullable,
        c.column_default,
        c.is_identity,
        CASE WHEN pk.constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_primary_key
      FROM information_schema.columns c
      LEFT JOIN (
        SELECT kcu.column_name, tc.constraint_type
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = ${raw(`'${table.id}'`)}
          AND tc.constraint_type = 'PRIMARY KEY'
      ) pk ON c.column_name = pk.column_name
      WHERE c.table_name = ${raw(`'${table.id}'`)}
    `);

    return mapSqlResult<any, TableColumnInfo, number>(columnResult, (row) => {
      return {
        name: row.column_name,
        type: {} as any,
        notNull: row.is_nullable === "NO",
        hasDefault: row.column_default !== null,
        isIdentity: row.is_identity === "YES",
        primaryKey: row.is_primary_key,
      };
    });
  }

  async executeUpdate<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>> {
    const sql = this.buildUpdateSql(table, values, where);
    return (await this.execute(sql)) as any;
  }

  async executeUpsert<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Promise<SqlResult<void, 0>> {
    const parsedValues = table.schema.parse(values);
    const sql = this.buildUpsertSql(table, parsedValues as any, field);
    return (await this.execute(sql)) as any;
  }

  async executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
    where: SelectCondition<ValueOfTable<TTable>> | undefined,
  ): Promise<SqlResult<Record<TKey, number>, 1>> {
    const statement = sql`SELECT ${raw(
      fields.map((field) =>
        raw(
          `COUNT(${field.key === "*" ? "*" : `ALL "${field.key}"`}) as ${field.key === "*" ? "_count" : `"${field.key}"`}`,
        ),
      ),
    )}
                          FROM ${table} ${
                            where
                              ? sql`WHERE
                          ${buildConditionSql(this, where, true)}`
                              : raw("")
                          }`;
    return this.execute(statement) as any;
  }

  async executeDelete<TTable extends Table>(
    table: TTable,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>> {
    const statement = sql`DELETE
                          FROM ${table}
                          WHERE ${buildConditionSql(this, where, true)}`;
    return this.execute(statement) as any;
  }

  protected buildUpdateSql<TTable extends Table>(
    table: TTable,
    valueMap: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Statement {
    const { keys, values } = Object.entries(valueMap).reduce(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc.keys.push(`"${key}"`);
          acc.values.push(valueToSql(value, true));
        }
        return acc;
      },
      { keys: [] as string[], values: [] as string[] },
    );

    return sql`UPDATE ${table}
               SET (${raw(keys)}) = (${raw(values)})
               WHERE ${buildConditionSql(this, where, true)} RETURNING *`;
  }

  protected buildUpsertSql<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<ValueOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Statement {
    return sql`INSERT INTO ${table.id} (${raw(Object.keys(values).map((k) => `"${k}"`))})
               VALUES (${raw(Object.values(values).map((v) => valueToSql(v, true)))}) ON CONFLICT ("${field.key}")
                 DO
    UPDATE SET ${raw(
      Object.entries(values).map(
        ([key, value]) => sql`"${key}"
      =
      ${valueToSql(value, true)}`,
      ),
    )}
      RETURNING *`;
  }

  async processDiff(table: Table, diff: TableDiff): Promise<void> {
    // do removes after adds so we don't end up with 0 columns
    diff.fields = diff.fields.sort((a, b) => TYPE_ORDERING[a.type] - TYPE_ORDERING[b.type]);

    for (const fieldDiff of diff.fields) {
      const field = fieldDiff.field;
      if (fieldDiff.type === "added") {
        const schema = field?.schema;
        if (!schema) {
          continue;
        }
        const primaryKeyMeta = getMetaItem(schema, primaryKey);

        const statement = sql`
          ALTER TABLE ${table.id}
            ADD COLUMN "${fieldDiff.key}" ${raw(this.typeToSql(schema))}${raw(primaryKeyMeta ? " PRIMARY KEY" : "")}`;

        await this.execute(statement);

        const backfillMeta = getRequiredBackfillMeta(schema);
        if (backfillMeta) {
          const backfillValue = backfillMeta.data.value;
          if (backfillValue === undefined || backfillValue === null) {
            throw new Error(`[zodbase] Backfill value is required when adding required field '${field?.key}'`);
          }

          await this.execute(
            sql`UPDATE ${table.id}
                SET "${fieldDiff.key}" = ${backfillValue}
                WHERE "${fieldDiff.key}" IS NULL`,
          );
        }
      } else if (fieldDiff.type === "removed") {
        await this.execute(sql`ALTER TABLE ${table.id} DROP COLUMN "${fieldDiff.key}"`);
      } else if (fieldDiff.type === "modified") {
        const schema = field?.schema;
        if (schema) {
          const backfillMeta = getRequiredBackfillMeta(schema);
          if (backfillMeta) {
            const backfillValue = backfillMeta.data.value;
            if (backfillValue === undefined || backfillValue === null) {
              throw new Error(`[zodbase] Backfill value is required when adding required field '${field?.key}'`);
            }

            await this.execute(
              sql`UPDATE ${table.id}
                  SET "${fieldDiff.key}" = ${backfillValue}
                  WHERE "${fieldDiff.key}" IS NULL`,
            );
          }
        }
      }
    }
  }

  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>> {
    const startTimestamp = Date.now();
    const statements = values.map((value) => {
      return this.driver.query(
        this.buildUpdateSql(table, value, field.equals(value[field.key] as any) as any)[TO_SQL_SYMBOL](),
      );
    });

    await Promise.all(statements);
    return {
      results: [],
      first: undefined,
      timings: {
        wallTimeMs: Date.now() - startTimestamp,
      },
    };
  }

  createTable(table: Table, name?: string) {
    const statement = sql`CREATE TABLE IF NOT EXISTS ${name ?? table.id}
                          (
                            ${join(
                              Object.values(table.fields).map((field) => {
                                const schema = field.schema;
                                const primaryKeyMeta = getMetaItem(schema, primaryKey);
                                //const autoIncrementMeta = getMetaItem(schema, autoIncrement);
                                return raw(
                                  [
                                    `"${field.key}"`,
                                    this.typeToSql(schema),
                                    primaryKeyMeta ? "PRIMARY KEY" : "",
                                    isZodRequired(schema) ? " NOT NULL" : "",
                                  ]
                                    .filter(Boolean)
                                    .join(" "),
                                );
                              }),
                              ",",
                            )}
                          )`;

    return this.execute(statement);
  }
}
