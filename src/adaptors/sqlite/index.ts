import type * as zod from "zod";
import { getMetaItem, type ZodMetaItem } from "zod-meta";
import {
  type BackfillOptions,
  backfill,
  type FieldDiffType,
  isZodRequired,
  mapSqlResult,
  raw,
  sql,
  type Table,
  type TableColumnInfo,
  type TableDiff,
  valueToSql,
} from "../..";
import DatabaseAdaptor, { type PossiblySelectedResult } from "../../DatabaseAdaptor";
import { quoteIdentifier } from "../../Escaping";
import {
  buildConditionSql,
  type InputOfTable,
  type SelectCondition,
  type SelectQuery,
  type SingleFieldBinding,
  type SqlDefiniteResult,
  type SqlResult,
  type StringKeys,
  type ValueOfTable,
} from "../../QueryBuilder";
import type { Statement } from "../../Statement";

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

export default abstract class SqliteAdaptor<TDriver> extends DatabaseAdaptor<TDriver> {
  buildJsonArrayContainsSql(fieldSql: string, value: unknown): Statement {
    return sql`EXISTS (SELECT 1 FROM json_each(${raw(fieldSql)}) WHERE value = ${value})`;
  }

  async processDiff(table: Table, diff: TableDiff): Promise<void> {
    let remake = false;
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
          remake = true;
        }
        if (isZodRequired(schema)) {
          remake = true;
        }
      } else if (fieldDiff.type === "removed") {
        await this.execute(sql`ALTER TABLE ${table.id}
            DROP COLUMN ${raw(quoteIdentifier(String(fieldDiff.key)))}`);
      } else if (fieldDiff.type === "modified") {
        remake = true;
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
        }
      }
    }

    if (remake) {
      // sqlite does not support modifying columns, so we need to create a new table
      const tempTableId = `${table.id}_temp_${crypto.randomUUID().split("-")[0]}`;
      await this.createTable(table, tempTableId);
      const columns = Object.keys(table.fields).map(quoteIdentifier).join(", ");
      await this.execute(
        sql`INSERT INTO ${raw(quoteIdentifier(tempTableId))} (${raw(columns)})
            SELECT ${raw(columns)} FROM ${table}`,
      );
      await this.execute(sql`DROP TABLE ${table.id}`);
      await this.execute(sql`ALTER TABLE ${raw(quoteIdentifier(tempTableId))} RENAME TO ${table}`);
    }
  }

  async syncTableIndexes(table: Table): Promise<void> {
    for (const index of table.indexes) {
      await this.execute(sql`
        CREATE ${raw(index.unique ? "UNIQUE " : "")}INDEX IF NOT EXISTS ${raw(quoteIdentifier(index.id))}
          ON ${table.id} (${raw(index.fields.map((field) => quoteIdentifier(String(field.key))).join(", "))})
          ${index.where ? sql`WHERE ${buildConditionSql(this, index.where, { includeTable: false })}` : raw("")}
      `);
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
            FROM ${select.table} ${select.where ? sql` WHERE ${buildConditionSql(this, select.where)}` : raw("")}${
              select.orderBy.length > 0
                ? sql` ORDER BY ${raw(
                    select.orderBy.map((order) => `${quoteIdentifier(String(order.field.key))} ${order.direction}`),
                  )}`
                : raw("")
            }${raw(select.limit !== undefined ? ` LIMIT ${select.limit}` : "")}${raw(
              select.offset !== undefined ? ` OFFSET ${select.offset}` : "",
            )}`;
  }

  executeSelect<R>(select: SelectQuery): R {
    const sql = this.buildSelectSql(select);
    return this.execute(sql) as any;
  }

  async executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
    const statement = sql`INSERT INTO ${table.id} (${raw(Object.keys(values as any).map(quoteIdentifier))})
                 VALUES ${Object.values(values as any)}`;
    await this.execute(statement);
    return {
      first: values as any,
      results: [values as any],
    };
  }

  async executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, number>> {
    const fieldKeys = Object.keys(table.fields);
    const statement = sql`INSERT INTO ${table.id} (${raw(fieldKeys.map(quoteIdentifier))})
                 VALUES ${raw(values.map((value: any) => sql`${fieldKeys.map((key) => value[key])}`))}`;
    await this.execute(statement);
    return {
      first: values[0] as any,
      results: values as any,
    };
  }

  async fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>> {
    const result = await this.execute(sql`PRAGMA table_info(${raw(table.id)})`);
    return mapSqlResult<any, TableColumnInfo, number>(result, (row) => {
      return {
        name: row.name,
        type: {} as any,
        notNull: row.notnull === 1,
        primaryKey: row.pk === 1,
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

  executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
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
                          ${buildConditionSql(this, where)}`
                              : ""
                          }`;
    return this.execute(statement) as any;
  }

  executeDelete<TTable extends Table>(
    table: TTable,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>> {
    const statement = sql`DELETE
                 FROM ${table}
                 WHERE ${buildConditionSql(this, where)}`;
    return this.execute(statement) as any;
  }

  protected buildUpdateSql<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
    shouldReturn = false,
  ): Statement {
    return sql`UPDATE ${table}
                 SET ${raw(
                   Object.entries(values).reduce((acc, [key, value]) => {
                     if (value !== undefined) {
                       acc.push(raw(`${quoteIdentifier(key)} = ${valueToSql(value, true)}`));
                     }
                     return acc;
                   }, [] as Statement[]),
                 )}
                 WHERE ${buildConditionSql(this, where)}${raw(shouldReturn ? " RETURNING *" : "")}`;
  }

  protected buildUpsertSql<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<ValueOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Statement {
    return sql`INSERT INTO ${table.id} (${raw(Object.keys(values).map(quoteIdentifier))})
                 VALUES ${Object.values(values)}
                 ON CONFLICT (${raw(quoteIdentifier(String(field.key)))})
                 DO UPDATE SET ${raw(
                   Object.entries(values).map(([key, value]) => sql`${raw(quoteIdentifier(key))} = ${value}`),
                 )}`;
  }
}
