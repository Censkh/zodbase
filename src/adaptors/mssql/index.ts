import * as z from "zod";
import { getMetaItem } from "zod-meta";
import DatabaseAdaptor, { type PossiblySelectedResult } from "../../DatabaseAdaptor";
import {
  backfill,
  foreignKey,
  isZodRequired,
  isZodTypeExtends,
  mapSqlResult,
  normalizeForeignKeyAction,
  primaryKey,
  raw,
  type Table,
  type TableColumnInfo,
  type TableDiff,
} from "../../index";
import {
  buildConditionSql,
  type InputOfTable,
  type SelectCondition,
  type SelectQuery,
  type SingleFieldBinding,
  type SqlResult,
  type StringKeys,
  type ValueOfTable,
} from "../../QueryBuilder";
import { isRelationalQuery } from "../../RelationalQuery";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";

/** Compatible with a connected node-mssql ConnectionPool or Transaction. */
export interface MssqlDriver {
  request(): { query(text: string): PromiseLike<{ recordset?: any[] }> };
  transaction?(): MssqlTransaction;
}
export interface MssqlTransaction extends MssqlDriver {
  begin(): PromiseLike<unknown>;
  commit(): PromiseLike<unknown>;
  rollback(): PromiseLike<unknown>;
}

export default class MssqlAdaptor extends DatabaseAdaptor<MssqlDriver> {
  override quoteIdentifier(value: string): string {
    return `[${value.replace(/]/g, "]]")}]`;
  }

  override valueToSql(value: unknown, nested = false): string {
    if (typeof value === "boolean") return value ? "1" : "0";
    if (Array.isArray(value) && !nested) return `(${value.map((item) => this.valueToSql(item, true)).join(", ")})`;
    const literal = super.valueToSql(value, nested);
    return literal.startsWith("'") ? `N${literal}` : literal;
  }

  override typeToSql(schema: z.ZodType): string {
    if (isZodTypeExtends(schema, z.ZodBoolean)) return "BIT";
    if (isZodTypeExtends(schema, z.ZodDate)) return "DATETIME2(3)";
    const base = super.typeToSql(schema);
    if (base === "JSONB" || isZodTypeExtends(schema, z.ZodRecord)) return "NVARCHAR(MAX)";
    if (base === "TEXT") return "NVARCHAR(450)";
    if (base === "REAL") return "FLOAT(53)";
    return base;
  }

  protected override selectFields(table: Table, fields?: SelectQuery["fields"]): string {
    return this.fieldsSql(table, fields);
  }

  private fieldsSql(table: Table, fields?: SelectQuery["fields"], prefix = ""): string {
    const expanded = fields
      ? fields.flatMap((field) => (field.key === "*" ? Object.values(table.fields) : [field]))
      : Object.values(table.fields);
    return expanded
      .map((field) => {
        const name = this.quoteIdentifier(String(field.key));
        const column = `${prefix}${name}`;
        if (isZodTypeExtends(field.schema, z.ZodDate)) return `CONVERT(NVARCHAR(30), ${column}, 126) AS ${name}`;
        if (isZodTypeExtends(field.schema, z.ZodBigInt)) return `CONVERT(NVARCHAR(30), ${column}) AS ${name}`;
        return column;
      })
      .join(", ");
  }

  protected override insertReturningSql(table: Table, position: "before-values" | "after-values"): string {
    return position === "before-values" ? ` OUTPUT ${this.fieldsSql(table, undefined, "INSERTED.")}` : "";
  }

  async execute(statement: Statement): Promise<SqlResult> {
    if (typeof statement?.[TO_SQL_SYMBOL] !== "function") throw new Error("Invalid statement");
    const text = statement[TO_SQL_SYMBOL]();
    const started = Date.now();
    let success = false;
    try {
      const result = await this.driver.request().query(text);
      success = true;
      const rows = result.recordset ?? [];
      return { results: rows, first: rows[0], timings: { wallTimeMs: Date.now() - started } };
    } finally {
      this.options.events?.onExecuteStatement?.({ sql: text, success, timings: { wallTimeMs: Date.now() - started } });
    }
  }

  override async transaction<T>(callback: (adaptor: DatabaseAdaptor) => Promise<T>): Promise<T> {
    if (!this.driver.transaction) throw new Error("Nested transactions are not supported");
    const transaction = this.driver.transaction();
    await transaction.begin();
    try {
      const result = await callback(new MssqlAdaptor({ ...this.options, driver: transaction }));
      await transaction.commit();
      return result;
    } catch (error) {
      await Promise.resolve(transaction.rollback()).catch(() => undefined);
      throw error;
    }
  }

  protected override selectDialect = "mssql" as const;

  override buildSelectSql(query: SelectQuery, scalar = false): Statement {
    if (isRelationalQuery(query)) return this.buildRelationalSelectSql(query, scalar);
    for (const value of [query.limit, query.offset]) {
      if (value !== undefined && (!Number.isSafeInteger(value) || value < 0))
        throw new Error("Invalid limit or offset");
    }
    const columns = scalar
      ? query.fields.map((field) => this.quoteIdentifier(String(field.key))).join(", ")
      : this.selectFields(query.table, query.fields);
    // FETCH NEXT requires a positive count; TOP (0) preserves an empty scalar result.
    const useOffset =
      (query.offset !== undefined || (scalar && query.orderBy.length > 0 && query.limit === undefined)) &&
      query.limit !== 0;
    const top = !useOffset && query.limit !== undefined ? `TOP (${query.limit}) ` : "";
    const order = query.orderBy
      .map(({ field, direction }) => `${this.quoteIdentifier(String(field.key))} ${direction}`)
      .join(", ");
    return raw(
      `SELECT ${top}${columns} FROM ${this.quoteIdentifier(String(query.table.id))}${query.where ? ` WHERE ${buildConditionSql(this, query.where)[TO_SQL_SYMBOL]()}` : ""}${order || useOffset ? ` ORDER BY ${order || "(SELECT NULL)"}` : ""}${useOffset ? ` OFFSET ${query.offset ?? 0} ROWS${query.limit !== undefined ? ` FETCH NEXT ${query.limit} ROWS ONLY` : ""}` : ""}`,
    );
  }

  async executeSelect<TTable extends Table, TLimit extends number>(
    query: SelectQuery<Table, TLimit>,
  ): Promise<SqlResult<ValueOfTable<TTable>, TLimit>> {
    if (isRelationalQuery(query)) return this.executeRelationalSelect(query) as any;
    return this.decodeResult(query.table, await this.execute(this.buildSelectSql(query))) as SqlResult<
      ValueOfTable<TTable>,
      TLimit
    >;
  }

  async executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>, 1>> {
    return this.executeInsertMany(table, [values], shouldReturn) as Promise<SqlResult<ValueOfTable<TTable>, 1>>;
  }

  async executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>>> {
    if (!values.length) return { results: [], first: undefined };
    if (values.length === 1 && Object.keys(values[0] as object).length === 0) {
      const result = await this.execute(
        raw(
          `INSERT INTO ${this.quoteIdentifier(String(table.id))}${shouldReturn ? this.insertReturningSql(table, "before-values") : ""} DEFAULT VALUES`,
        ),
      );
      return this.decodeResult(table, result) as SqlResult<ValueOfTable<TTable>>;
    }
    return this.executeInsertSubqueries(table, values as Record<string, unknown>[], shouldReturn) as Promise<
      SqlResult<ValueOfTable<TTable>>
    >;
  }

  async executeUpdate<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
    shouldReturn = false,
  ): Promise<PossiblySelectedResult<ValueOfTable<TTable>>> {
    const assignments = Object.entries(values)
      .filter(([, value]) => value !== undefined)
      .map(([key, value]) => `${this.quoteIdentifier(key)} = ${this.valueToSql(value, true)}`)
      .join(", ");
    const result = await this.execute(
      raw(
        `UPDATE ${this.quoteIdentifier(String(table.id))} SET ${assignments}${shouldReturn ? this.insertReturningSql(table, "before-values") : ""} WHERE ${buildConditionSql(this, where)[TO_SQL_SYMBOL]()}`,
      ),
    );
    return { ...this.decodeResult(table, result), selected: shouldReturn } as PossiblySelectedResult<
      ValueOfTable<TTable>
    >;
  }

  async executeUpsert<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    field: SingleFieldBinding<ValueOfTable<TTable>, TKey>,
  ): Promise<SqlResult<void, 0>> {
    const keys = Object.keys(values);
    const q = (key: string) => this.quoteIdentifier(key);
    // HOLDLOCK gives serializable key-range protection against concurrent insert races.
    const text = `MERGE ${q(String(table.id))} WITH (HOLDLOCK) AS target USING (VALUES (${keys.map((key) => this.valueToSql((values as any)[key], true)).join(", ")})) AS source (${keys.map(q).join(", ")}) ON target.${q(String(field.key))} = source.${q(String(field.key))} WHEN MATCHED THEN UPDATE SET ${keys.map((key) => `target.${q(key)} = source.${q(key)}`).join(", ")} WHEN NOT MATCHED THEN INSERT (${keys.map(q).join(", ")}) VALUES (${keys.map((key) => `source.${q(key)}`).join(", ")})${this.insertReturningSql(table, "before-values")};`;
    return this.decodeResult(table, await this.execute(raw(text))) as SqlResult<void, 0>;
  }

  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & z.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(table: TTable, values: TValue[], field: SingleFieldBinding<TValue, TKey>): Promise<SqlResult<void, 0>> {
    // node-mssql permits only one in-flight request on a transaction connection.
    for (const value of values) await this.executeUpdate(table, value, field.equals(value[field.key] as any) as any);
    return { results: [], first: undefined };
  }

  async executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
    where: SelectCondition<ValueOfTable<TTable>> | undefined,
  ): Promise<SqlResult<Record<TKey, number>, 1>> {
    const columns = fields
      .map(
        (field) =>
          `COUNT_BIG(${field.key === "*" ? "*" : this.quoteIdentifier(String(field.key))}) AS ${this.quoteIdentifier(field.key === "*" ? "_count" : String(field.key))}`,
      )
      .join(", ");
    const result = await this.execute(
      raw(
        `SELECT ${columns} FROM ${this.quoteIdentifier(String(table.id))}${where ? ` WHERE ${buildConditionSql(this, where)[TO_SQL_SYMBOL]()}` : ""}`,
      ),
    );
    return mapSqlResult(result, (row: Record<string, unknown>) =>
      Object.fromEntries(Object.entries(row).map(([key, value]) => [key, Number(value)])),
    ) as SqlResult<Record<TKey, number>, 1>;
  }

  async executeDelete<TTable extends Table>(
    table: TTable,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>> {
    await this.execute(
      raw(
        `DELETE FROM ${this.quoteIdentifier(String(table.id))} WHERE ${buildConditionSql(this, where)[TO_SQL_SYMBOL]()}`,
      ),
    );
    return { results: [], first: undefined };
  }

  buildJsonArrayContainsSql(fieldSql: string, value: unknown): Statement {
    if (value !== null && !["string", "number", "boolean"].includes(typeof value))
      throw new Error("SQL Server JSON contains supports scalar array elements only");
    const type = value === null ? 0 : typeof value === "string" ? 1 : typeof value === "number" ? 2 : 3;
    const comparison =
      value === null
        ? "[value] IS NULL"
        : typeof value === "number"
          ? `TRY_CONVERT(FLOAT(53), [value]) = ${this.valueToSql(value)}`
          : `[value] COLLATE Latin1_General_100_BIN2 = ${this.valueToSql(String(value))}`;
    return raw(`EXISTS (SELECT 1 FROM OPENJSON(${fieldSql}) WHERE [type] = ${type} AND ${comparison})`);
  }

  async createTable(table: Table, name?: string): Promise<SqlResult> {
    const tableName = this.quoteIdentifier(name ?? String(table.id));
    const columns = Object.values(table.fields).map((field) => {
      const schema = field.schema;
      const fk = getMetaItem(schema, foreignKey)?.data;
      const action = fk?.onDelete === "restrict" ? "no action" : (fk?.onDelete ?? "no action");
      return `${this.quoteIdentifier(String(field.key))} ${this.typeToSql(schema)} ${isZodRequired(schema) ? "NOT NULL" : "NULL"}${getMetaItem(schema, primaryKey) ? " PRIMARY KEY" : ""}${fk ? ` REFERENCES ${this.quoteIdentifier(String(fk.field.table.id))} (${this.quoteIdentifier(String(fk.field.key))}) ON DELETE ${action.toUpperCase()}` : ""}`;
    });
    return this.execute(
      raw(
        `IF OBJECT_ID(${this.valueToSql(tableName)}, N'U') IS NULL CREATE TABLE ${tableName} (${columns.join(", ")})`,
      ),
    );
  }

  async fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>> {
    const result = await this.execute(
      raw(`SELECT c.name, TYPE_NAME(c.user_type_id) AS sql_type, c.is_nullable, c.is_identity, c.default_object_id,
      CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id WHERE i.object_id=c.object_id AND i.is_primary_key=1 AND ic.column_id=c.column_id) THEN 1 ELSE 0 END AS is_primary_key,
      fk.name AS fk_name, rt.name AS referenced_table, rc.name AS referenced_column, fk.delete_referential_action_desc AS delete_rule
      FROM sys.columns c
      LEFT JOIN sys.foreign_key_columns fkc ON fkc.parent_object_id=c.object_id AND fkc.parent_column_id=c.column_id
      LEFT JOIN sys.foreign_keys fk ON fk.object_id=fkc.constraint_object_id
      LEFT JOIN sys.tables rt ON rt.object_id=fkc.referenced_object_id
      LEFT JOIN sys.columns rc ON rc.object_id=fkc.referenced_object_id AND rc.column_id=fkc.referenced_column_id
      WHERE c.object_id=OBJECT_ID(${this.valueToSql(this.quoteIdentifier(String(table.id)))}, N'U') ORDER BY c.column_id`),
    );
    return mapSqlResult(result, (row: any) => ({
      name: row.name,
      type: {} as any,
      sqlType: row.sql_type,
      notNull: !row.is_nullable,
      hasDefault: Boolean(row.default_object_id),
      isIdentity: Boolean(row.is_identity),
      primaryKey: Boolean(row.is_primary_key),
      ...(row.fk_name
        ? {
            foreignKey: {
              table: row.referenced_table,
              field: row.referenced_column,
              constraintName: row.fk_name,
              onDelete:
                row.delete_rule === "NO_ACTION" &&
                table.fields[row.name] &&
                getMetaItem(table.fields[row.name]!.schema, foreignKey)?.data.onDelete === "restrict"
                  ? "restrict"
                  : normalizeForeignKeyAction(row.delete_rule.replaceAll("_", " ")),
            },
          }
        : {}),
    }));
  }

  async syncTableIndexes(table: Table): Promise<void> {
    const target = this.quoteIdentifier(String(table.id));
    for (const index of table.indexes) {
      const fields = index.fields.map((field) => this.quoteIdentifier(String(field.key))).join(", ");
      await this.execute(
        raw(
          `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(${this.valueToSql(target)}) AND name=${this.valueToSql(index.id)}) CREATE ${index.unique ? "UNIQUE " : ""}INDEX ${this.quoteIdentifier(index.id)} ON ${target} (${fields})${index.where ? ` WHERE ${buildConditionSql(this, index.where, { includeTable: false })[TO_SQL_SYMBOL]()}` : ""}`,
        ),
      );
    }
  }

  async processDiff(table: Table, diff: TableDiff): Promise<void> {
    // SQL Server DDL is transactional: failed validation/backfill must leave no partial migration.
    await this.transaction(async (base) => {
      const adaptor = base as MssqlAdaptor;
      const target = this.quoteIdentifier(String(table.id));
      for (const change of [...diff.fields].sort(
        (a, b) => Number(a.type === "removed") - Number(b.type === "removed"),
      )) {
        const column = this.quoteIdentifier(String(change.key));
        const schema = change.field?.schema;
        if (change.type === "removed") {
          await adaptor.execute(raw(`ALTER TABLE ${target} DROP COLUMN ${column}`));
          continue;
        }
        if (!schema) continue;
        if (change.type === "added")
          await adaptor.execute(raw(`ALTER TABLE ${target} ADD ${column} ${this.typeToSql(schema)} NULL`));
        const required = isZodRequired(schema);
        const altersNull =
          change.type === "added" ||
          change.modifications?.some(
            (mod) =>
              (mod.type === "add-constraint" || mod.type === "remove-constraint") && mod.constraint === "NOT NULL",
          );
        if (altersNull && required) {
          const value = getMetaItem(schema, backfill)?.data.value;
          if (value != null)
            await adaptor.execute(
              raw(`UPDATE ${target} SET ${column} = ${this.valueToSql(value, true)} WHERE ${column} IS NULL`),
            );
          else if (
            (await adaptor.execute(raw(`SELECT TOP (1) 1 AS present FROM ${target} WHERE ${column} IS NULL`))).results
              .length
          )
            throw new Error(`[zodbase] Backfill value is required when adding required field '${String(change.key)}'`);
        }
        if (altersNull)
          await adaptor.execute(
            raw(
              `ALTER TABLE ${target} ALTER COLUMN ${column} ${this.typeToSql(schema)} ${required ? "NOT NULL" : "NULL"}`,
            ),
          );
        if (
          (change.type === "added" && getMetaItem(schema, primaryKey)) ||
          change.modifications?.some((mod) => mod.type === "add-constraint" && mod.constraint === "PRIMARY KEY")
        )
          await adaptor.execute(raw(`ALTER TABLE ${target} ADD PRIMARY KEY (${column})`));
        const fk = getMetaItem(schema, foreignKey)?.data;
        const additions =
          change.type === "added" && fk
            ? [
                {
                  type: "add-foreign-key" as const,
                  foreignKey: {
                    table: String(fk.field.table.id),
                    field: String(fk.field.key),
                    onDelete: fk.onDelete ?? "no action",
                  },
                },
              ]
            : [];
        for (const mod of [...(change.modifications ?? []), ...additions]) {
          if (mod.type === "remove-foreign-key") {
            if (!mod.foreignKey.constraintName) throw new Error("SQL Server foreign key constraint name is required");
            await adaptor.execute(
              raw(`ALTER TABLE ${target} DROP CONSTRAINT ${this.quoteIdentifier(mod.foreignKey.constraintName)}`),
            );
          } else if (mod.type === "add-foreign-key") {
            const action = mod.foreignKey.onDelete === "restrict" ? "NO ACTION" : mod.foreignKey.onDelete.toUpperCase();
            await adaptor.execute(
              raw(
                `ALTER TABLE ${target} ADD CONSTRAINT ${this.quoteIdentifier(`${String(table.id)}_${String(change.key)}_fkey`)} FOREIGN KEY (${column}) REFERENCES ${this.quoteIdentifier(mod.foreignKey.table)} (${this.quoteIdentifier(mod.foreignKey.field)}) ON DELETE ${action}`,
              ),
            );
          } else if (mod.type === "widen-number" || mod.type === "widen-date") {
            await adaptor.execute(
              raw(
                `ALTER TABLE ${target} ALTER COLUMN ${column} ${this.typeToSql(schema)} ${required ? "NOT NULL" : "NULL"}`,
              ),
            );
          } else if (
            (mod.type === "add-constraint" || mod.type === "remove-constraint") &&
            mod.constraint !== "NOT NULL" &&
            !(mod.type === "add-constraint" && mod.constraint === "PRIMARY KEY")
          ) {
            throw new Error(`Unsupported SQL Server constraint change: ${mod.type} ${mod.constraint}`);
          }
        }
      }
    });
  }
}
