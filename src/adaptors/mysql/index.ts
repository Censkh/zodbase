import type { Connection, Pool, RowDataPacket } from "mysql2/promise";
import * as zod from "zod";
import { getMetaItem, type ZodMetaItem } from "zod-meta";
import DatabaseAdaptor, {
  type Conflict,
  type DatabaseAdaptorOptions,
  keyCondition,
  type PossiblySelectedResult,
} from "../../DatabaseAdaptor";
import { quoteMysqlIdentifier } from "../../Escaping";
import {
  type BackfillOptions,
  backfill,
  computed,
  type ExecuteStatementEvent,
  type FieldDiffType,
  foreignKey,
  isZodRequired,
  isZodTypeExtends,
  join,
  mapSqlResult,
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
import { isRelationalQuery, type SelectDialect } from "../../RelationalQuery";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import { type TableIndex, writableFieldKeys } from "../../Table";

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
  protected override tablelessSelectSql = " FROM DUAL";

  // MySQL converts a selected value to its column's type on insert, and its CAST names other types.
  protected override insertSelectValueSql(value: string): string {
    return value;
  }
  protected override supportsSubqueryReturning = false;
  override readonly updateReturnsRows = false;
  override async transaction<TResult>(callback: (adaptor: DatabaseAdaptor) => Promise<TResult>): Promise<TResult> {
    if ("getConnection" in this.driver) {
      const connection = await this.driver.getConnection();
      const Adaptor = this.constructor as new (options: DatabaseAdaptorOptions<Connection>) => DatabaseAdaptor;
      const adaptor = new Adaptor(this.optionsFor(connection));
      try {
        return await adaptor.transaction(callback);
      } finally {
        connection.release();
      }
    }
    return super.transaction(callback);
  }

  protected override textCastType = "CHAR";

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
      return "DATETIME(3)";
    }
    if (isZodTypeExtends(type, zod.ZodString)) {
      return "VARCHAR(255)";
    }
    return super.typeToSql(type);
  }

  buildJsonArrayContainsSql(fieldSql: string, value: unknown): Statement {
    return sql`JSON_CONTAINS(${raw(fieldSql)}, JSON_ARRAY(${value}))`;
  }

  private sessionReady: Promise<unknown> | undefined;

  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") {
      throw new Error("Invalid statement");
    }

    if ("getConnection" in this.driver) {
      const connection = await this.driver.getConnection();
      try {
        const Adaptor = this.constructor as new (options: DatabaseAdaptorOptions<Connection>) => MysqlAdaptor;
        return await new Adaptor(this.optionsFor(connection)).execute(statement);
      } finally {
        connection.release();
      }
    }
    // Our SQL literals use standard doubled quotes. MySQL must not reinterpret backslashes. Literals
    // take the tables' collation, so comparing or combining them with columns never mixes two.
    await (this.sessionReady ??= this.driver
      .query("SET SESSION sql_mode = CONCAT_WS(',', @@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES')")
      .then(() => this.driver.query(`SET NAMES utf8mb4 COLLATE ${this.textCollation}`)));

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
    return value;
  }

  protected override selectDialect: SelectDialect = "mysql";

  override buildSelectSql(select: SelectQuery, scalar = false, named = false): Statement {
    if (isRelationalQuery(select)) return this.buildRelationalSelectSql(select, scalar, named);
    const tableName = this.quoteIdentifier(String(select.table.id));
    const offsetSql = select.offset === undefined ? "" : ` OFFSET ${select.offset}`;
    const limitSql =
      select.limit !== undefined
        ? ` LIMIT ${select.limit}`
        : select.offset !== undefined
          ? " LIMIT 18446744073709551615"
          : "";

    return sql`SELECT ${raw(scalar ? select.fields.map((field) => this.quoteIdentifier(String(field.key))).join(", ") : this.selectFields(select.table, select.fields))}
      FROM ${raw(tableName)}${select.where ? sql` WHERE ${buildConditionSql(this, select.where)}` : raw("")}${
        select.orderBy.length > 0
          ? sql` ORDER BY ${raw(
              select.orderBy.map(
                (order) =>
                  `${this.quoteIdentifier(String(select.table.id))}.${this.quoteIdentifier(String(order.field.key))} ${order.direction}`,
              ),
            )}`
          : raw("")
      }${raw(limitSql)}${raw(offsetSql)}`;
  }

  async executeSelect<R>(select: SelectQuery): Promise<R> {
    if (isRelationalQuery(select)) return this.executeRelationalSelect(select) as any;
    return this.decodeResult(select.table, await this.execute(this.buildSelectSql(select))) as R;
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
    const fieldKeys = writableFieldKeys(table);
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
        c.column_type AS column_type,
        c.is_nullable AS is_nullable,
        c.column_default AS column_default,
        c.extra AS extra,
        CASE WHEN c.column_key = 'PRI' THEN 1 ELSE 0 END AS is_primary_key,
        fk.constraint_name AS foreign_key_constraint_name,
        fk.referenced_table_name AS foreign_table_name,
        fk.referenced_column_name AS foreign_column_name,
        rc.delete_rule AS delete_rule
      FROM information_schema.columns c
      LEFT JOIN information_schema.key_column_usage fk
        ON fk.table_schema = c.table_schema
        AND fk.table_name = c.table_name
        AND fk.column_name = c.column_name
        AND fk.referenced_table_name IS NOT NULL
      LEFT JOIN information_schema.referential_constraints rc
        ON rc.constraint_schema = fk.constraint_schema
        AND rc.constraint_name = fk.constraint_name
      WHERE c.table_schema = DATABASE()
        AND c.table_name = ${String(table.id)}
      ORDER BY c.ordinal_position
    `);

    return mapSqlResult<any, TableColumnInfo, number>(result, (row) => ({
      name: row.column_name,
      type: {} as any,
      sqlType: row.column_type,
      notNull: row.is_nullable === "NO",
      hasDefault: row.column_default !== null,
      isIdentity: String(row.extra).includes("auto_increment"),
      primaryKey: Boolean(row.is_primary_key),
      generated: /\bGENERATED\b/i.test(String(row.extra)),
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

  async executeUpsert<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    conflict: Conflict,
  ): Promise<SqlResult<void, 0>> {
    const keys = Object.keys(values);
    const statement = sql`INSERT INTO ${raw(this.quoteIdentifier(String(table.id)))} (${raw(
      keys.map((key) => this.quoteIdentifier(key)),
    )}) VALUES (${raw(Object.values(values).map((value) => valueToSql(value, true)))})${raw(
      this.onConflictSql(conflict, keys),
    )}`;
    return this.execute(statement) as Promise<SqlResult<void, 0>>;
  }

  // The selected rows, read through a derived table of their own: the conflict's assignments name
  // the target table's columns, which a select of that same table would make ambiguous.
  protected override insertSelectSource(select: Statement, conflict?: Conflict): Statement {
    return conflict ? sql`SELECT * FROM (${select}) AS ${raw(this.quoteIdentifier("_zodbase_rows"))}` : select;
  }

  /**
   * MySQL matches a row on any of the table's unique keys (`ON DUPLICATE KEY UPDATE`), so it can't
   * name a partial index; skipping sets a key column to itself, and `incoming(field)` is `VALUES(field)`.
   */
  protected override onConflictSql(conflict: Conflict, columns: readonly string[]): string {
    if (conflict.where) throw new Error("[zodbase] MySQL can't name a partial index as a conflict target");
    const quote = (name: string) => this.quoteIdentifier(name);
    if (conflict.action === "nothing") {
      // Qualified, as a select's columns may share its name.
      const [field] = conflict.fields;
      const key = `${quote(String(field!.table.id))}.${quote(String(field!.key))}`;
      return ` ON DUPLICATE KEY UPDATE ${key} = ${key}`;
    }
    return ` ON DUPLICATE KEY UPDATE ${this.conflictAssignments(conflict, columns, (column) => `VALUES(${quote(column)})`)}`;
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
        .map(([key, value]) => `${this.quoteIdentifier(key)} = ${this.valueToSql(value, true)}`)
        .join(", "),
    )} WHERE ${buildConditionSql(this, where)}`;
  }

  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(
    table: TTable,
    values: TValue[],
    fields: readonly SingleFieldBinding<TValue, TKey>[],
    where?: SelectCondition,
  ): Promise<SqlResult<void, 0>> {
    const startTimestamp = Date.now();
    await Promise.all(
      values.map((value) => this.execute(this.buildUpdateSql(table, value, keyCondition(fields, value, where) as any))),
    );
    return { results: [], first: undefined, timings: { wallTimeMs: Date.now() - startTimestamp } };
  }

  async processDiff(table: Table, diff: TableDiff): Promise<void> {
    for (const change of diff.fields)
      if (change.type === "added" && change.field && getMetaItem(change.field.schema, computed))
        throw new Error(
          `[zodbase] This database can't add the computed column '${String(change.key)}' to an existing table`,
        );
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
          await this.execute(
            sql`UPDATE ${tableSql} SET ${columnSql} = ${raw(valueToSql(backfillValue, true))} WHERE ${columnSql} IS NULL`,
          );
        }
        if (isZodRequired(schema)) {
          await this.execute(
            sql`ALTER TABLE ${tableSql} MODIFY COLUMN ${columnSql} ${raw(this.typeToSql(schema))} NOT NULL`,
          );
        }
        if (this.isInlinePrimaryKey(table, fieldDiff)) {
          await this.execute(sql`ALTER TABLE ${tableSql} ADD PRIMARY KEY (${columnSql})`);
        }
        const foreignKeyMeta = getMetaItem(schema, foreignKey);
        if (foreignKeyMeta) {
          const constraintName = `${String(table.id)}_${String(fieldDiff.key)}_fkey`;
          await this.execute(sql`ALTER TABLE ${tableSql}
              ADD CONSTRAINT ${raw(this.quoteIdentifier(constraintName))}
              FOREIGN KEY (${columnSql})
              REFERENCES ${raw(this.quoteIdentifier(String(foreignKeyMeta.data.field.table.id)))} (${raw(
                this.quoteIdentifier(String(foreignKeyMeta.data.field.key)),
              )})
              ON DELETE ${raw((foreignKeyMeta.data.onDelete ?? "no action").toUpperCase())}`);
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
          await this.execute(
            sql`UPDATE ${tableSql} SET ${columnSql} = ${raw(valueToSql(backfillValue, true))} WHERE ${columnSql} IS NULL`,
          );
        }
        for (const modification of fieldDiff.modifications ?? []) {
          if (modification.type === "add-constraint" || modification.type === "remove-constraint") {
            if (modification.type === "add-constraint" && modification.constraint === "PRIMARY KEY") {
              await this.execute(sql`ALTER TABLE ${tableSql} ADD PRIMARY KEY (${columnSql})`);
              continue;
            }
            if (modification.constraint !== "NOT NULL") {
              continue;
            }
            await this.execute(
              sql`ALTER TABLE ${tableSql} MODIFY COLUMN ${columnSql} ${raw(this.typeToSql(schema))} ${raw(
                modification.type === "add-constraint" ? "NOT NULL" : "NULL",
              )}`,
            );
          } else if (modification.type === "widen-date") {
            await this.execute(
              sql`ALTER TABLE ${tableSql} MODIFY COLUMN ${columnSql} DATETIME(3) ${raw(isZodRequired(schema) ? "NOT NULL" : "NULL")}`,
            );
          } else if (modification.type === "remove-foreign-key") {
            const constraintName =
              modification.foreignKey.constraintName ?? `${String(table.id)}_${String(fieldDiff.key)}_fkey`;
            await this.execute(
              sql`ALTER TABLE ${tableSql} DROP FOREIGN KEY ${raw(this.quoteIdentifier(constraintName))}`,
            );
          } else if (modification.type === "add-foreign-key") {
            const constraintName = `${String(table.id)}_${String(fieldDiff.key)}_fkey`;
            await this.execute(sql`ALTER TABLE ${tableSql}
                ADD CONSTRAINT ${raw(this.quoteIdentifier(constraintName))}
                FOREIGN KEY (${columnSql})
                REFERENCES ${raw(this.quoteIdentifier(modification.foreignKey.table))} (${raw(
                  this.quoteIdentifier(modification.foreignKey.field),
                )})
                ON DELETE ${raw(modification.foreignKey.onDelete.toUpperCase())}`);
          }
        }
      }
    }
    if (diff.primaryKey) {
      const tableSql = raw(this.quoteIdentifier(String(table.id)));
      const columns = raw(diff.primaryKey.map((key) => this.quoteIdentifier(key)).join(", "));
      const { results } = await this.execute(sql`
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = DATABASE() AND table_name = ${String(table.id)} AND constraint_type = 'PRIMARY KEY'`);
      await this.execute(
        results.length
          ? sql`ALTER TABLE ${tableSql} DROP PRIMARY KEY, ADD PRIMARY KEY (${columns})`
          : sql`ALTER TABLE ${tableSql} ADD PRIMARY KEY (${columns})`,
      );
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
        await this.execute(this.createIndexStatement(table, index));
      }
    }
  }

  createIndexStatement(table: Table, index: TableIndex): Statement {
    if (index.where) {
      throw new Error("MySQL and MariaDB do not support partial indexes");
    }
    return sql`
          CREATE ${raw(index.unique ? "UNIQUE " : "")}INDEX ${raw(this.quoteIdentifier(index.id))}
          ON ${raw(this.quoteIdentifier(String(table.id)))} (${raw(
            index.fields.map((field) => this.quoteIdentifier(String(field.key))).join(", "),
          )})
        `;
  }

  override createTableStatement(table: Table, name?: string): Statement {
    const constraint = this.primaryKeyConstraintSql(table);
    return sql`CREATE TABLE IF NOT EXISTS ${raw(this.quoteIdentifier(name ?? String(table.id)))} (
      ${join(
        Object.values(table.fields)
          .map((field) => {
            const foreignKeyMeta = getMetaItem(field.schema, foreignKey);
            const typeSql = this.typeToSql(field.schema);
            return raw(
              [
                this.quoteIdentifier(String(field.key)),
                typeSql,
                this.computedColumnSql(table, field),
                this.isInlinePrimaryKey(table, field) ? "PRIMARY KEY" : "",
                // MariaDB refuses NOT NULL on a generated column; its expression decides.
                isZodRequired(field.schema) && !getMetaItem(field.schema, computed) ? "NOT NULL" : "",
                foreignKeyMeta
                  ? `REFERENCES ${this.quoteIdentifier(String(foreignKeyMeta.data.field.table.id))} (${this.quoteIdentifier(String(foreignKeyMeta.data.field.key))}) ON DELETE ${(foreignKeyMeta.data.onDelete ?? "no action").toUpperCase()}`
                  : "",
              ]
                .filter(Boolean)
                .join(" "),
            );
          })
          .concat(constraint ? [raw(constraint)] : []),
        ", ",
      )}
    ) DEFAULT CHARSET=utf8mb4 COLLATE=${raw(this.textCollation)}`;
  }

  /**
   * Text compares as it does in the other databases: by its exact characters, so `'a'` isn't `'A'`
   * and `'a '` isn't `'a'`. MySQL's default collation ignores case and trailing spaces.
   */
  protected textCollation = "utf8mb4_0900_bin";

  // Text compares case-sensitively, so a case-insensitive match lowercases both sides.
  override buildPatternMatchSql(fieldSql: string, pattern: string, negate: boolean): Statement {
    return sql`LOWER(${raw(fieldSql)}) ${raw(negate ? "NOT LIKE" : "LIKE")} ${this.toLikePattern(pattern.toLowerCase())} ESCAPE '!'`;
  }
}
