import type * as zod from "zod";
import { getMetaItem, type ZodMetaItem } from "zod-meta";
import DatabaseAdaptor, {
  type DatabaseAdaptorOptions,
  groupUpsertRows,
  type PossiblySelectedResult,
} from "../../DatabaseAdaptor";
import { quoteIdentifier } from "../../Escaping";
import {
  type BackfillOptions,
  backfill,
  computed,
  type ExecuteStatementEvent,
  type FieldDiffType,
  foreignKey,
  isZodRequired,
  join,
  mapSqlResult,
  monotonicTimestamp,
  normalizeForeignKeyAction,
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
import { isRelationalQuery } from "../../RelationalQuery";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import { type SchemaTableInfo, type TableIndex, writableFieldKeys } from "../../Table";

type BackfillMetaItem = ZodMetaItem<BackfillOptions>;

/** The query surface shared by node-postgres and Neon WebSocket clients. */
export interface PostgresClient {
  query(text: string): PromiseLike<{ rows: any[] }>;
}
export interface PostgresPool extends PostgresClient {
  readonly totalCount: number;
  connect(): Promise<PostgresClient & { release(): void }>;
}

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
  TDriver extends PostgresClient | PostgresPool = PostgresClient | PostgresPool,
> extends DatabaseAdaptor<TDriver> {
  override async transaction<TResult>(callback: (adaptor: DatabaseAdaptor) => Promise<TResult>): Promise<TResult> {
    if ("totalCount" in this.driver) {
      const client = await (this.driver as PostgresPool).connect();
      const Adaptor = this.constructor as new (options: DatabaseAdaptorOptions<PostgresClient>) => DatabaseAdaptor;
      const adaptor = new Adaptor({ ...this.options, driver: client });
      try {
        return await adaptor.transaction(callback);
      } finally {
        client.release();
      }
    }
    return super.transaction(callback);
  }

  override typeToSql(type: zod.ZodType<any>): string {
    const sqlType = super.typeToSql(type);
    // JavaScript numbers require 64-bit precision; REAL loses millisecond revisions.
    return sqlType === "REAL" ? "DOUBLE PRECISION" : sqlType;
  }

  buildJsonArrayContainsSql(fieldSql: string, value: unknown): Statement {
    return sql`${raw(fieldSql)} @> ${raw(valueToSql([value], true))}::jsonb`;
  }

  // LIKE is case-sensitive in PostgreSQL; ILIKE matches the other databases.
  override buildPatternMatchSql(fieldSql: string, pattern: string, negate: boolean): Statement {
    return sql`${raw(fieldSql)} ${raw(negate ? "NOT ILIKE" : "ILIKE")} ${this.toLikePattern(pattern)} ESCAPE '!'`;
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
    return value;
  }

  protected override materializeRepeatedSubqueries = true;

  protected override selectDialect = "postgres" as const;

  /** The table a select reads; PostgreSQL takes no index hints. */
  protected selectSource(select: SelectQuery): Statement {
    return sql`${select.table}`;
  }

  override buildSelectSql(select: SelectQuery, scalar = false, named = false): Statement {
    if (isRelationalQuery(select)) return this.buildRelationalSelectSql(select, scalar, named);
    return sql`SELECT ${raw(scalar ? select.fields.map((field) => this.quoteIdentifier(String(field.key))).join(", ") : this.selectFields(select.table, select.fields))}
               FROM ${this.selectSource(select)} ${
                 select.where
                   ? sql` WHERE
               ${buildConditionSql(this, select.where, true)}`
                   : raw("")
               }${
                 select.orderBy.length > 0
                   ? sql` ORDER BY
                   ${raw(
                     select.orderBy.map(
                       (order) =>
                         `${this.quoteIdentifier(String(select.table.id))}.${this.quoteIdentifier(String(order.field.key))} ${order.direction}`,
                     ),
                   )}`
                   : raw("")
               }${raw(select.limit !== undefined ? ` LIMIT ${select.limit}` : "")}${raw(
                 select.offset !== undefined ? ` OFFSET ${select.offset}` : "",
               )}`;
  }

  async executeSelect<R>(select: SelectQuery): Promise<R> {
    if (isRelationalQuery(select)) return this.executeRelationalSelect(select) as any;
    const sql = this.buildSelectSql(select);
    return this.decodeResult(select.table, await this.execute(sql)) as any;
  }

  async executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>, 1>> {
    const statement = sql`INSERT INTO ${table.id} (${raw(Object.keys(values as any).map(quoteIdentifier))})
                          VALUES (${raw(Object.values(values as any).map((v) => valueToSql(v, true)))})${raw(
                            shouldReturn ? ` RETURNING ${this.selectFields(table)}` : "",
                          )}`;
    if (shouldReturn) {
      return this.decodeResult(table, await this.execute(statement)) as any;
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
    const statement = this.buildInsertStatement(table, values as Record<string, unknown>[], {
      returning: shouldReturn,
    });
    if (shouldReturn) {
      return this.decodeResult(table, await this.execute(statement)) as any;
    }
    await this.execute(statement);
    return {
      first: undefined,
      results: [],
    };
  }

  override buildInsertStatement(
    table: Table,
    rows: readonly Record<string, unknown>[],
    options: { returning: boolean; conflict?: { fields: readonly SingleFieldBinding[]; action: "nothing" | "update" } },
  ): Statement {
    const columns = writableFieldKeys(table);
    const { conflict } = options;
    const target = conflict?.fields.map((field) => quoteIdentifier(String(field.key))).join(", ");
    const onConflict = !conflict
      ? ""
      : conflict.action === "nothing"
        ? ` ON CONFLICT (${target}) DO NOTHING`
        : ` ON CONFLICT (${target}) DO UPDATE SET ${columns
            .map((key) => `${quoteIdentifier(key)} = excluded.${quoteIdentifier(key)}`)
            .join(", ")}`;
    return sql`INSERT INTO ${table.id} (${raw(columns.map(quoteIdentifier))})
                          VALUES ${raw(this.valueTuples(rows, columns))}${raw(onConflict)}${raw(
                            options.returning ? ` RETURNING ${this.selectFields(table)}` : "",
                          )}`;
  }

  override buildUpdateStatement(
    table: Table,
    values: Record<string, unknown>,
    where: SelectCondition,
    returning: boolean,
  ): Statement {
    return this.buildUpdateSql(table, values as any, where as any, returning);
  }

  override buildDeleteStatement(table: Table, where: SelectCondition): Statement {
    return sql`DELETE
                          FROM ${table}
                          WHERE ${buildConditionSql(this, where, true)}`;
  }

  /**
   * One simple-protocol query of every statement: PostgreSQL and CockroachDB run a query of several
   * statements as one implicit transaction, and answer with each statement's rows.
   */
  override async executeTogether(statements: readonly Statement[]): Promise<SqlResult[] | undefined> {
    if (statements.length === 1) return [await this.execute(statements[0]!)];
    const startTimestamp = Date.now();
    const rawSql = statements.map((statement) => statement[TO_SQL_SYMBOL]()).join(";\n");
    let success = false;
    let timings: SqlResultTimings | undefined;
    try {
      const response = await this.driver.query(rawSql);
      success = true;
      timings = { wallTimeMs: Date.now() - startTimestamp };
      const responses = (Array.isArray(response) ? response : [response]) as { rows?: any[] }[];
      if (responses.length !== statements.length)
        throw new Error("[zodbase] The database answered a batch with a different number of results");
      return responses.map((result) => {
        const rows = result.rows ?? [];
        return this.mapResult({ results: rows, first: rows[0], timings });
      });
    } finally {
      this.options.events?.onExecuteStatement?.({
        sql: rawSql,
        timings: timings ?? { wallTimeMs: Date.now() - startTimestamp },
        success,
      });
    }
  }

  override async describeSchema(): Promise<SchemaTableInfo[]> {
    const tables = new Map<string, SchemaTableInfo>();
    const tableResult = await this.execute(
      raw(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'",
      ),
    );
    for (const row of tableResult.results)
      tables.set(String(row.table_name), { name: String(row.table_name), columns: [], indexes: [] });
    const columnResult = await this.execute(
      raw(
        `SELECT table_name, column_name FROM information_schema.columns
         WHERE table_schema = current_schema() ${this.visibleColumnCondition} ORDER BY table_name, ordinal_position`,
      ),
    );
    for (const row of columnResult.results) tables.get(String(row.table_name))?.columns.push(String(row.column_name));
    // Unique constraints are backed by an index of the same name, except CockroachDB's
    // UNIQUE WITHOUT INDEX, so both lists are read and merged by name.
    const indexResult = await this.execute(
      raw(
        `SELECT t.relname AS table_name, i.relname AS index_name, ix.indisunique AS is_unique
         FROM pg_catalog.pg_index ix
         JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
         JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
         JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
         WHERE n.nspname = current_schema() AND NOT ix.indisprimary
         UNION ALL
         SELECT t.relname AS table_name, c.conname AS index_name, true AS is_unique
         FROM pg_catalog.pg_constraint c
         JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
         JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
         WHERE n.nspname = current_schema() AND c.contype = 'u'`,
      ),
    );
    for (const row of indexResult.results) {
      const table = tables.get(String(row.table_name));
      const name = String(row.index_name);
      if (!table || table.indexes.some((index) => index.name === name)) continue;
      table.indexes.push({ name, unique: row.is_unique === true || row.is_unique === "t" });
    }
    return [...tables.values()];
  }

  /** Narrows the information_schema.columns query to the columns a table's rows show. */
  protected visibleColumnCondition = "";

  async fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>> {
    const columnResult = await this.execute(sql`
      SELECT 
        c.column_name,
        c.data_type,
        c.is_nullable,
        c.column_default,
        c.is_identity,
        c.is_generated,
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
        sqlType: row.data_type,
        notNull: row.is_nullable === "NO",
        hasDefault: row.column_default !== null,
        isIdentity: row.is_identity === "YES",
        primaryKey: row.is_primary_key,
        generated: row.is_generated === "ALWAYS",
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
    const result = this.decodeResult(table, await this.execute(sql)) as PossiblySelectedResult<ValueOfTable<TTable>>;
    result.selected = shouldReturn;
    return result;
  }

  async executeUpsert<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    fields: readonly SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
  ): Promise<SqlResult<void, 0>> {
    const sql = this.buildUpsertSql(table, values as any, fields);
    return this.decodeResult(table, await this.execute(sql)) as any;
  }

  override async executeUpsertMany<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>[],
    fields: readonly SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
  ): Promise<SqlResult<void, 0>> {
    const target = raw(fields.map((field) => quoteIdentifier(String(field.key))).join(", "));
    for (const rows of groupUpsertRows(
      values as Record<string, unknown>[],
      fields.map((field) => String(field.key)),
    )) {
      const keys = Object.keys(rows[0]!);
      await this.execute(sql`INSERT INTO ${table.id} (${raw(keys.map(quoteIdentifier))})
                             VALUES ${raw(rows.map((row) => sql`(${raw(keys.map((key) => valueToSql(row[key], true)))})`))}
                             ON CONFLICT (${target})
                             DO UPDATE SET ${raw(keys.map((key) => raw(`${quoteIdentifier(key)} = excluded.${quoteIdentifier(key)}`)))}`);
    }
    return { results: [], first: undefined };
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
    return this.execute(this.buildDeleteStatement(table, where)) as any;
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
                     assignments.push(`${quoteIdentifier(key)} = ${this.valueToSql(value, true)}`);
                   }
                   return assignments;
                 }, [] as string[]),
               )}
               WHERE ${buildConditionSql(this, where, true)}${raw(shouldReturn ? ` RETURNING ${this.selectFields(table)}` : "")}`;
  }

  protected buildUpsertSql<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<ValueOfTable<TTable>>,
    fields: readonly SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
  ): Statement {
    const target = raw(fields.map((field) => quoteIdentifier(String(field.key))).join(", "));
    return sql`INSERT INTO ${table.id} (${raw(Object.keys(values).map(quoteIdentifier))})
               VALUES (${raw(Object.values(values).map((v) => valueToSql(v, true)))}) ON CONFLICT (${target})
                 DO
    UPDATE SET ${raw(
      Object.entries(values).map(
        ([key, value]) => sql`${raw(quoteIdentifier(key))}
      =
      ${raw(valueToSql(value, true))}`,
      ),
    )}
      RETURNING ${raw(this.selectFields(table))}`;
  }

  async processDiff(table: Table, diff: TableDiff): Promise<void> {
    let tableHasRows: boolean | undefined;
    for (const fieldDiff of diff.fields) {
      const schema = fieldDiff.field?.schema;
      // The database fills a computed column in itself.
      const addsRequiredField =
        fieldDiff.type === "added" && schema && isZodRequired(schema) && !getMetaItem(schema, computed);
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
        const primaryKeyMeta = this.isInlinePrimaryKey(table, fieldDiff);
        const computedSql = this.computedColumnSql(table, fieldDiff.field!);
        if (computedSql) {
          await this.execute(sql`
            ALTER TABLE ${table.id}
              ADD COLUMN ${raw(quoteIdentifier(String(fieldDiff.key)))} ${raw(this.typeToSql(schema))} ${raw(computedSql)}${raw(isZodRequired(schema) ? " NOT NULL" : "")}`);
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
                SET ${raw(quoteIdentifier(String(fieldDiff.key)))} = ${raw(valueToSql(backfillValue, true))}
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
          await this.addPrimaryKey(table, String(fieldDiff.key));
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
                  SET ${raw(quoteIdentifier(String(fieldDiff.key)))} = ${raw(valueToSql(backfillValue, true))}
                  WHERE ${raw(quoteIdentifier(String(fieldDiff.key)))} IS NULL`,
            );
          }

          for (const modification of fieldDiff.modifications ?? []) {
            if (modification.type === "add-constraint" || modification.type === "remove-constraint") {
              if (modification.type === "add-constraint" && modification.constraint === "PRIMARY KEY") {
                await this.addPrimaryKey(table, String(fieldDiff.key));
                continue;
              }
              if (modification.constraint !== "NOT NULL") {
                continue;
              }
              await this.execute(
                sql`ALTER TABLE ${table.id}
                    ALTER COLUMN ${raw(quoteIdentifier(String(fieldDiff.key)))} ${raw(
                      modification.type === "add-constraint" ? "SET NOT NULL" : "DROP NOT NULL",
                    )}`,
              );
            } else if (modification.type === "widen-number") {
              const column = quoteIdentifier(String(fieldDiff.key));
              await this.transaction(async (adaptor) => {
                // Read using the old wire type: Cockroach can store more precision than it sends.
                const previous = getMetaItem(schema, monotonicTimestamp)
                  ? (await adaptor.execute(sql`SELECT MAX(${raw(column)}) AS revision FROM ${table.id}`)).first
                      ?.revision
                  : undefined;
                await adaptor.execute(sql`ALTER TABLE ${table.id}
                  ALTER COLUMN ${raw(column)} TYPE DOUBLE PRECISION`);
                if (previous != null) {
                  const revision = Math.max(Date.now(), Number(previous)) + 1;
                  await adaptor.execute(sql`UPDATE ${table.id} SET ${raw(column)} =
                    GREATEST(${raw(column)} + 1, ${revision})`);
                }
              });
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
    if (diff.primaryKey) await this.setPrimaryKey(table, diff.primaryKey);
  }

  protected async addPrimaryKey(table: Table, key: string): Promise<void> {
    await this.execute(sql`ALTER TABLE ${table.id} ADD PRIMARY KEY (${raw(quoteIdentifier(key))})`);
  }

  /**
   * Makes `keys` the table's primary key, in order. The old constraint is replaced in the same
   * statement, so CockroachDB keeps no unique index of the old key (as ALTER PRIMARY KEY would).
   */
  protected async setPrimaryKey(table: Table, keys: string[]): Promise<void> {
    const { results } = await this.execute(sql`
      SELECT constraint_name FROM information_schema.table_constraints
      WHERE table_name = ${String(table.id)} AND table_schema = current_schema() AND constraint_type = 'PRIMARY KEY'`);
    const current = (results[0] as { constraint_name?: string } | undefined)?.constraint_name;
    const name = current ?? `${String(table.id)}_pkey`;
    const columns = raw(keys.map((key) => quoteIdentifier(key)).join(", "));
    await this.execute(
      current
        ? sql`ALTER TABLE ${table.id} DROP CONSTRAINT ${raw(quoteIdentifier(current))}, ADD CONSTRAINT ${raw(quoteIdentifier(name))} PRIMARY KEY (${columns})`
        : sql`ALTER TABLE ${table.id} ADD CONSTRAINT ${raw(quoteIdentifier(name))} PRIMARY KEY (${columns})`,
    );
  }

  async syncTableIndexes(table: Table): Promise<void> {
    for (const index of table.indexes) {
      await this.execute(this.createIndexStatement(table, index));
    }
  }

  createIndexStatement(table: Table, index: TableIndex): Statement {
    return sql`
        CREATE ${raw(index.unique ? "UNIQUE " : "")}INDEX IF NOT EXISTS ${raw(quoteIdentifier(index.id))}
          ON ${table.id} (${raw(index.fields.map((field) => quoteIdentifier(String(field.key))).join(", "))})
          ${index.where ? sql`WHERE ${buildConditionSql(this, index.where, { doubleQuote: true, includeTable: false })}` : raw("")}
      `;
  }

  /**
   * One statement per set of columns the rows change, joining the rows to the table on their key:
   * `UPDATE t SET c = v.c FROM (VALUES …) AS v (k, c) WHERE t.k = v.k`. The database finds and
   * writes them together, where statements of their own each wait on their own row (on CockroachDB,
   * a write's consensus in its region). The values are cast to their columns' types, which a
   * VALUES list doesn't carry.
   */
  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], fields: readonly SingleFieldBinding<TValue, TKey>[]): Promise<SqlResult<void, 0>> {
    const startTimestamp = Date.now();
    const keys = fields.map((field) => String(field.key));
    const tableFields = table.fields as Record<string, { schema: zod.ZodType }>;
    const cast = (column: string) => `v.${quoteIdentifier(column)}::${this.typeToSql(tableFields[column]!.schema)}`;
    const rows = values.map((value) =>
      Object.fromEntries(Object.entries(value as Record<string, unknown>).filter(([, item]) => item !== undefined)),
    );
    for (const group of groupUpsertRows(rows, keys)) {
      const columns = Object.keys(group[0]!).filter((column) => !keys.includes(column));
      if (!columns.length) continue;
      const names = [...keys, ...columns];
      await this.execute(
        sql`UPDATE ${table} SET ${raw(columns.map((column) => `${quoteIdentifier(column)} = ${cast(column)}`).join(", "))}
            FROM (VALUES ${raw(group.map((row) => `(${names.map((name) => this.valueToSql(row[name], true)).join(", ")})`).join(", "))})
              AS v (${raw(names.map(quoteIdentifier).join(", "))})
            WHERE ${raw(keys.map((key) => `${quoteIdentifier(String(table.id))}.${quoteIdentifier(key)} = ${cast(key)}`).join(" AND "))}`,
      );
    }
    return {
      results: [],
      first: undefined,
      timings: {
        wallTimeMs: Date.now() - startTimestamp,
      },
    };
  }

  override createTableStatement(table: Table, name?: string): Statement {
    const constraint = this.primaryKeyConstraintSql(table);
    return sql`CREATE TABLE IF NOT EXISTS ${raw(quoteIdentifier(name ?? String(table.id)))}
                          (
                            ${join(
                              Object.values(table.fields)
                                .map((field) => {
                                  const schema = field.schema;
                                  const foreignKeyMeta = getMetaItem(schema, foreignKey);
                                  //const autoIncrementMeta = getMetaItem(schema, autoIncrement);
                                  return raw(
                                    [
                                      quoteIdentifier(String(field.key)),
                                      this.typeToSql(schema),
                                      this.computedColumnSql(table, field),
                                      this.isInlinePrimaryKey(table, field) ? "PRIMARY KEY" : "",
                                      isZodRequired(schema) ? "NOT NULL" : "",
                                      foreignKeyMeta
                                        ? `REFERENCES ${quoteIdentifier(String(foreignKeyMeta.data.field.table.id))} (${quoteIdentifier(String(foreignKeyMeta.data.field.key))}) ON DELETE ${(foreignKeyMeta.data.onDelete ?? "no action").toUpperCase()}`
                                        : "",
                                    ]
                                      .filter(Boolean)
                                      .join(" "),
                                  );
                                })
                                .concat(constraint ? [raw(constraint)] : []),
                              ", ",
                            )}
                          )`;
  }
}
