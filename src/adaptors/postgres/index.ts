import type * as pg from "pg";
import type * as zod from "zod";
import { getMetaItem, type ZodMetaItem } from "zod-meta";
import DatabaseAdaptor, { type DatabaseAdaptorOptions, type PossiblySelectedResult } from "../../DatabaseAdaptor";
import { quoteIdentifier } from "../../Escaping";
import {
  type BackfillOptions,
  backfill,
  type ExecuteStatementEvent,
  type FieldDiffType,
  foreignKey,
  isZodRequired,
  join,
  mapSqlResult,
  normalizeForeignKeyAction,
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

export default class PostgresAdaptor<
  TDriver extends pg.Client | pg.Pool = pg.Client | pg.Pool,
> extends DatabaseAdaptor<TDriver> {
  override async transaction<TResult>(callback: (adaptor: DatabaseAdaptor) => Promise<TResult>): Promise<TResult> {
    if ("totalCount" in this.driver) {
      const client = await this.driver.connect();
      const Adaptor = this.constructor as new (options: DatabaseAdaptorOptions<pg.PoolClient>) => DatabaseAdaptor;
      const adaptor = new Adaptor({ ...this.options, driver: client });
      try {
        return await adaptor.transaction(callback);
      } finally {
        client.release();
      }
    }
    return super.transaction(callback);
  }

  buildJsonArrayContainsSql(fieldSql: string, value: unknown): Statement {
    return sql`${raw(fieldSql)} @> ${raw(valueToSql([value], true))}::jsonb`;
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
    return sql`SELECT ${raw(
      select.fields.map((field) => (field.key === "*" ? "*" : quoteIdentifier(String(field.key)))),
    )}
               FROM ${select.table} ${
                 select.where
                   ? sql` WHERE
               ${buildConditionSql(this, select.where, true)}`
                   : raw("")
}${
                 select.orderBy.length > 0
                   ? sql` ORDER BY
                   ${raw(
                     select.orderBy.map((order) => `${quoteIdentifier(String(order.field.key))} ${order.direction}`),
                   )}`
                   : raw("")
}${raw(select.limit !== undefined ? ` LIMIT ${select.limit}` : "")}${raw(
                 select.offset !== undefined ? ` OFFSET ${select.offset}` : "",
               )}`;
  }

  async executeSelect<R>(select: SelectQuery): Promise<R> {
    const sql = this.buildSelectSql(select);
    return this.execute(sql) as any;
  }

  async executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>, 1>> {
    const statement = sql`INSERT INTO ${table.id} (${raw(Object.keys(values as any).map(quoteIdentifier))})
                          VALUES (${raw(Object.values(values as any).map((v) => valueToSql(v, true)))})${raw(
                            shouldReturn ? " RETURNING *" : "",
                          )}`;
    if (shouldReturn) {
      return (await this.execute(statement)) as any;
    }
    await this.execute(statement);
    return {
      first: undefined,
      results: [],
    };
  }

  async executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>, number>> {
    const fieldKeys = Object.keys(table.fields);
    const statement = sql`INSERT INTO ${table.id} (${raw(fieldKeys.map(quoteIdentifier))})
                          VALUES ${raw(
                            values.map(
                              (value: any) => sql`(${raw(fieldKeys.map((key) => valueToSql(value[key], true)))})`,
                            ),
                          )}${raw(shouldReturn ? " RETURNING *" : "")}`;
    if (shouldReturn) {
      return (await this.execute(statement)) as any;
    }
    await this.execute(statement);
    return {
      first: undefined,
      results: [],
    };
  }

  async fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>> {
    const columnResult = await this.execute(sql`
      SELECT 
        c.column_name,
        c.is_nullable,
        c.column_default,
        c.is_identity,
        CASE WHEN pk.constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_primary_key,
        fk.constraint_name AS foreign_key_constraint_name,
        fk.foreign_table_name,
        fk.foreign_column_name,
        fk.delete_rule
      FROM information_schema.columns c
      LEFT JOIN (
        SELECT kcu.column_name, tc.constraint_type
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_schema = kcu.constraint_schema
          AND tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = ${String(table.id)}
          AND tc.table_schema = current_schema()
          AND tc.constraint_type = 'PRIMARY KEY'
      ) pk ON c.column_name = pk.column_name
      LEFT JOIN (
        SELECT
          kcu.column_name,
          tc.constraint_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name,
          rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_schema = kcu.constraint_schema
          AND tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_schema = ccu.constraint_schema
          AND tc.constraint_name = ccu.constraint_name
        JOIN information_schema.referential_constraints rc
          ON tc.constraint_schema = rc.constraint_schema
          AND tc.constraint_name = rc.constraint_name
        WHERE tc.table_name = ${String(table.id)}
          AND tc.table_schema = current_schema()
          AND tc.constraint_type = 'FOREIGN KEY'
      ) fk ON c.column_name = fk.column_name
      WHERE c.table_name = ${String(table.id)}
        AND c.table_schema = current_schema()
    `);

    return mapSqlResult<any, TableColumnInfo, number>(columnResult, (row) => {
      return {
        name: row.column_name,
        type: {} as any,
        notNull: row.is_nullable === "NO",
        hasDefault: row.column_default !== null,
        isIdentity: row.is_identity === "YES",
        primaryKey: row.is_primary_key,
        ...(row.foreign_table_name
          ? {
              foreignKey: {
                table: row.foreign_table_name,
                field: row.foreign_column_name,
                onDelete: normalizeForeignKeyAction(row.delete_rule),
                constraintName: row.foreign_key_constraint_name,
              },
            }
          : {}),
      };
    });
  }

  async executeUpdate<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
    shouldReturn = false,
  ): Promise<PossiblySelectedResult<ValueOfTable<TTable>>> {
    const sql = this.buildUpdateSql(table, values, where, shouldReturn);
    const result = (await this.execute(sql)) as PossiblySelectedResult<ValueOfTable<TTable>>;
    result.selected = shouldReturn;
    return result;
  }

  async executeUpsert<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Promise<SqlResult<void, 0>> {
    const sql = this.buildUpsertSql(table, values as any, field);
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
          `COUNT(${field.key === "*" ? "*" : `ALL ${quoteIdentifier(String(field.key))}`}) as ${quoteIdentifier(
            field.key === "*" ? "_count" : String(field.key),
          )}`,
        ),
      ),
    )}
                          FROM ${table} ${
                            where
                              ? sql`WHERE
                          ${buildConditionSql(this, where, true)}`
                              : raw("")
                          }`;
    const result = await this.execute(statement);
    return mapSqlResult(result, (row: Record<string, string | number>) =>
      Object.fromEntries(Object.entries(row).map(([key, value]) => [key, Number(value)])),
    ) as any;
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
    shouldReturn = false,
  ): Statement {
    return sql`UPDATE ${table}
               SET ${raw(
                 Object.entries(valueMap).reduce((assignments, [key, value]) => {
                   if (value !== undefined) {
                     assignments.push(`${quoteIdentifier(key)} = ${valueToSql(value, true)}`);
                   }
                   return assignments;
                 }, [] as string[]),
               )}
               WHERE ${buildConditionSql(this, where, true)}${raw(shouldReturn ? " RETURNING *" : "")}`;
  }

  protected buildUpsertSql<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<ValueOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Statement {
    return sql`INSERT INTO ${table.id} (${raw(Object.keys(values).map(quoteIdentifier))})
               VALUES (${raw(Object.values(values).map((v) => valueToSql(v, true)))}) ON CONFLICT (${raw(
                 quoteIdentifier(String(field.key)),
               )})
                 DO
    UPDATE SET ${raw(
      Object.entries(values).map(
        ([key, value]) => sql`${raw(quoteIdentifier(key))}
      =
      ${raw(valueToSql(value, true))}`,
      ),
    )}
      RETURNING *`;
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
        tableHasRows ??= (await this.execute(sql`SELECT 1 FROM ${table} LIMIT 1`)).results.length > 0;
        requiresBackfill = tableHasRows;
      } else if (addsRequiredConstraint) {
        requiresBackfill =
          (
            await this.execute(
              sql`SELECT 1 FROM ${table}
                  WHERE ${raw(quoteIdentifier(String(fieldDiff.key)))} IS NULL LIMIT 1`,
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
            ADD COLUMN ${raw(quoteIdentifier(String(fieldDiff.key)))} ${raw(this.typeToSql(schema))}`;

        await this.execute(statement);

        const backfillMeta = getRequiredBackfillMeta(schema);
        if (backfillMeta) {
          const backfillValue = backfillMeta.data.value;
          if (backfillValue === undefined || backfillValue === null) {
            throw new Error(`[zodbase] Backfill value is required when adding required field '${field?.key}'`);
          }

          await this.execute(
            sql`UPDATE ${table.id}
                SET ${raw(quoteIdentifier(String(fieldDiff.key)))} = ${backfillValue}
                WHERE ${raw(quoteIdentifier(String(fieldDiff.key)))} IS NULL`,
          );
        }
        if (isZodRequired(schema)) {
          await this.execute(
            sql`ALTER TABLE ${table.id}
                ALTER COLUMN ${raw(quoteIdentifier(String(fieldDiff.key)))} SET NOT NULL`,
          );
        }
        if (primaryKeyMeta) {
          await this.execute(
            sql`ALTER TABLE ${table.id}
                ADD PRIMARY KEY (${raw(quoteIdentifier(String(fieldDiff.key)))})`,
          );
        }
        const foreignKeyMeta = getMetaItem(schema, foreignKey);
        if (foreignKeyMeta) {
          const constraintName = `${String(table.id)}_${String(fieldDiff.key)}_fkey`;
          await this.execute(sql`ALTER TABLE ${table.id}
              ADD CONSTRAINT ${raw(quoteIdentifier(constraintName))}
              FOREIGN KEY (${raw(quoteIdentifier(String(fieldDiff.key)))})
              REFERENCES ${foreignKeyMeta.data.field.table.id} (${foreignKeyMeta.data.field.key})
              ON DELETE ${raw((foreignKeyMeta.data.onDelete ?? "no action").toUpperCase())}`);
        }
      } else if (fieldDiff.type === "removed") {
        await this.execute(sql`ALTER TABLE ${table.id} DROP COLUMN ${raw(quoteIdentifier(String(fieldDiff.key)))}`);
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
                  SET ${raw(quoteIdentifier(String(fieldDiff.key)))} = ${backfillValue}
                  WHERE ${raw(quoteIdentifier(String(fieldDiff.key)))} IS NULL`,
            );
          }

          for (const modification of fieldDiff.modifications ?? []) {
            if (modification.type === "add-constraint" || modification.type === "remove-constraint") {
              if (modification.constraint !== "NOT NULL") {
                continue;
              }
              await this.execute(
                sql`ALTER TABLE ${table.id}
                    ALTER COLUMN ${raw(quoteIdentifier(String(fieldDiff.key)))} ${raw(
                      modification.type === "add-constraint" ? "SET NOT NULL" : "DROP NOT NULL",
                    )}`,
              );
            } else if (modification.type === "remove-foreign-key") {
              const constraintName =
                modification.foreignKey.constraintName ?? `${String(table.id)}_${String(fieldDiff.key)}_fkey`;
              await this.execute(sql`ALTER TABLE ${table.id} DROP CONSTRAINT ${raw(quoteIdentifier(constraintName))}`);
            } else if (modification.type === "add-foreign-key") {
              const constraintName = `${String(table.id)}_${String(fieldDiff.key)}_fkey`;
              await this.execute(sql`ALTER TABLE ${table.id}
                  ADD CONSTRAINT ${raw(quoteIdentifier(constraintName))}
                  FOREIGN KEY (${raw(quoteIdentifier(String(fieldDiff.key)))})
                  REFERENCES ${raw(quoteIdentifier(modification.foreignKey.table))} (${raw(
                    quoteIdentifier(modification.foreignKey.field),
                  )})
                  ON DELETE ${raw(modification.foreignKey.onDelete.toUpperCase())}`);
            }
          }
        }
      }
    }
  }

  async syncTableIndexes(table: Table): Promise<void> {
    for (const index of table.indexes) {
      await this.execute(sql`
        CREATE ${raw(index.unique ? "UNIQUE " : "")}INDEX IF NOT EXISTS ${raw(quoteIdentifier(index.id))}
          ON ${table.id} (${raw(index.fields.map((field) => quoteIdentifier(String(field.key))).join(", "))})
          ${index.where ? sql`WHERE ${buildConditionSql(this, index.where, { doubleQuote: true, includeTable: false })}` : raw("")}
      `);
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
        this.buildUpdateSql(table, value, field.equals(value[field.key] as any) as any, false)[TO_SQL_SYMBOL](),
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
                                    isZodRequired(schema) ? "NOT NULL" : "",
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
