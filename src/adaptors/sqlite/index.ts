import { getMetaItem } from "zod-meta";
import {
  type Table,
  type TableColumnInfo,
  type TableDiff,
  backfill,
  isZodRequired,
  mapSqlResult,
  primaryKey,
  raw,
  sql,
} from "../..";
import DatabaseAdaptor from "../../DatabaseAdaptor";
import {
  type InputOfTable,
  type SelectCondition,
  type SelectQuery,
  type SingleFieldBinding,
  type SqlDefiniteResult,
  type SqlResult,
  type StringKeys,
  type ValueOfTable,
  buildConditionSql,
} from "../../QueryBuilder";
import type { Statement } from "../../Statement";

const JSON_START = /[{\[]/;
const JSON_END = /[\]}]/;

export default abstract class SqliteAdaptor<TDriver> extends DatabaseAdaptor<TDriver> {
  async processDiff(table: Table, diff: TableDiff): Promise<void> {
    let hasModified = false;

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
              ADD COLUMN ${fieldDiff.key} ${this.typeToSql(schema)}${
                primaryKeyMeta ? " PRIMARY KEY" : ""
              }`;

        await this.execute(statement);

        // try to backfill
        if (isZodRequired(schema)) {
          const backfillMeta = getMetaItem(schema, backfill);
          if (backfillMeta) {
            const backfillValue = backfillMeta.data.value;
            if (!backfillValue) {
              throw new Error("[zod-sql] Backfill value is required when adding a required field");
            }

            await this.execute(
              sql`UPDATE ${table.id}
                 SET ${raw(fieldDiff.key)} = ${backfillValue} WHERE ${raw(fieldDiff.key)} IS NULL`,
            );

            await this.execute(
              sql`ALTER TABLE ${table.id} ALTER COLUMN ${fieldDiff.key} SET NOT NULL`,
            );
          }
        }
      } else if (fieldDiff.type === "removed") {
        await this.execute(sql`ALTER TABLE ${table.id}
            DROP COLUMN ${fieldDiff.key}`);
      } else if (fieldDiff.type === "modified") {
        hasModified = true;
      }
    }

    if (hasModified) {
      // sqlite does not support modifying columns, so we need to create a new table
      const tempTableId = `${table.id}_temp_${crypto.randomUUID().split("-")[0]}`;
      await this.createTable(table, tempTableId);
      await this.execute(sql`INSERT INTO ${raw(tempTableId)} SELECT * FROM ${table}`);
      await this.execute(sql`DROP TABLE ${table.id}`);
      await this.execute(sql`ALTER TABLE ${raw(tempTableId)} RENAME TO ${table}`);
    }
  }

  protected mapResult(value: SqlResult): SqlResult {
    return mapSqlResult(value, (value) => {
      return Object.fromEntries(
        Object.entries(value).map(([key, value]) => {
          if (
            typeof value === "string" &&
            JSON_START.test(value[0]) &&
            JSON_END.test(value[value.length - 1])
          ) {
            try {
              // @ts-ignore
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
    return sql`SELECT ${raw(select.fields.map((field) => `${field.key}`))}
            FROM ${select.table} ${
              select.where ? sql` WHERE ${buildConditionSql(this, select.where)}` : raw("")
            }${
              select.orderBy.length > 0
                ? sql` ORDER BY ${raw(
                    select.orderBy.map((order) => `${order.field} ${order.direction}`),
                  )}`
                : raw("")
            }${raw(select.limit ? ` LIMIT ${select.limit}` : "")}`;
  }

  executeSelect<R>(select: SelectQuery): R {
    const sql = this.buildSelectSql(select);
    return this.execute(sql) as any;
  }

  async executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
    const parsedValues = table.schema.parse(values);
    const statement = sql`INSERT INTO ${table.id} (${raw(Object.keys(parsedValues))})
                 VALUES ${Object.values(parsedValues)}`;
    await this.execute(statement);
    return {
      first: parsedValues as any,
      results: [parsedValues as any],
    };
  }

  async executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, number>> {
    const parsedValues = values.map((value) => table.schema.parse(value));
    const statement = sql`INSERT INTO ${table.id} (${raw(Object.keys(parsedValues[0]))})
                 VALUES ${raw(parsedValues.map((value) => sql`${Object.values(value)}`))}`;
    await this.execute(statement);
    return {
      first: parsedValues[0] as any,
      results: parsedValues as any,
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

  executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
    where: SelectCondition<ValueOfTable<TTable>> | undefined,
  ): Promise<SqlResult<Record<TKey, number>, 1>> {
    const statement = sql`SELECT ${raw(
      fields.map((field) => raw(`COUNT(ALL ${field.key}) as ${field.key}`)),
    )}
                 FROM ${table} ${where ? sql`WHERE ${buildConditionSql(this, where)}` : ""}`;
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
    values: Partial<ValueOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Statement {
    const statement = sql`UPDATE ${table}
                 SET ${raw(Object.entries(values).map(([key, value]) => sql`${raw(key)} = ${value}`))}
                 WHERE ${buildConditionSql(this, where)}`;
    return statement;
  }

  protected buildUpsertSql<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<ValueOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Statement {
    return sql`INSERT INTO ${table.id} (${raw(Object.keys(values))})
                 VALUES ${Object.values(values)}
                 ON CONFLICT (${raw(field.key)})
                 DO UPDATE SET ${raw(
                   Object.entries(values).map(([key, value]) => sql`${raw(key)} = ${value}`),
                 )}`;
  }
}
