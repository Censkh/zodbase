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
  raw,
  sql,
  type Table,
  type TableColumnInfo,
  type TableDiff,
  valueToSql,
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
import { isScalarSelect, SELECT_QUERY } from "./QueryBuilder";
import type { QueryStatement } from "./QueryStatement";
import { compileRelationalQuery, type SelectDialect } from "./RelationalQuery";
import { divideSql, SqlExpression } from "./SqlExpression";
import { type Statement, TO_SQL_SYMBOL } from "./Statement";
import { canShareSubquery, isSubquery } from "./Subquery";
import { getPrimaryKey, type SchemaTableInfo, type TableIndex } from "./Table";

export interface DatabaseAdaptorOptions<TDriver = any> {
  driver: TDriver;
  debug?: boolean;
  events?: DatabaseEvents;
}

export interface PossiblySelectedResult<TValue = any, TLimit extends number = number>
  extends SqlResult<TValue, TLimit> {
  selected: boolean;
}

/**
 * Rows to upsert, grouped by the columns they set so each group can share one statement. A key
 * given more than once keeps its last row, as upserting them in turn would.
 */
export const groupUpsertRows = <TRow extends Record<string, unknown>>(
  rows: readonly TRow[],
  keys: readonly string[],
) => {
  const latest = new Map<string, TRow>();
  for (const row of rows) {
    const key = JSON.stringify(keys.map((name) => row[name]));
    latest.delete(key);
    latest.set(key, row);
  }
  const groups = new Map<string, TRow[]>();
  for (const row of latest.values()) {
    const columns = JSON.stringify(Object.keys(row).sort());
    groups.set(columns, [...(groups.get(columns) ?? []), row]);
  }
  return [...groups.values()];
};

export default abstract class DatabaseAdaptor<TDriver = any> {
  private transactionQueue: Promise<void> = Promise.resolve();

  constructor(protected readonly options: DatabaseAdaptorOptions<TDriver>) {}

  protected get driver() {
    return this.options.driver;
  }

  quoteIdentifier(value: string): string {
    return quoteIdentifier(value);
  }

  valueToSql(value: unknown, nested = false): string {
    return value instanceof SqlExpression ? value.toSql(this) : valueToSql(value, nested);
  }

  divideSql(dividend: string, divisor: string): string {
    return divideSql(dividend, divisor);
  }

  subtractSql(left: string, right: string): string {
    return `(${left} - ${right})`;
  }

  protected insertReturningSql(table: Table, position: "before-values" | "after-values"): string {
    return position === "after-values" ? ` RETURNING ${this.selectFields(table)}` : "";
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

  decodeSelectedValue(schema: zod.ZodType | undefined, value: any): any {
    if (!schema || value == null) return value;
    if (isZodTypeExtends(schema, zod.ZodBigInt)) return BigInt(value);
    const numberType = isZodTypeExtends(schema, zod.ZodNumber);
    if (numberType && (typeof value === "string" || typeof value === "bigint")) {
      // PostgreSQL INT8/NUMERIC arrive as strings; keep the declared number contract on reads.
      const number = typeof value === "string" && !value.trim() ? Number.NaN : Number(value);
      return numberType.parse(number);
    }
    if (isZodTypeExtends(schema, zod.ZodBoolean)) return value === true || value === 1 || value === BigInt(1);
    if (isZodTypeExtends(schema, zod.ZodDate)) {
      if (value instanceof Date) return value;
      const text = String(value).replace(" ", "T");
      return new Date(/[zZ]|[+-]\d\d(?::?\d\d)?$/.test(text) ? text : `${text}Z`);
    }
    if (
      typeof value === "string" &&
      (isZodTypeExtends(schema, zod.ZodObject) ||
        isZodTypeExtends(schema, zod.ZodArray) ||
        isZodTypeExtends(schema, zod.ZodRecord))
    )
      return JSON.parse(value);
    if (
      typeof value === "string" &&
      (isZodTypeExtends(schema, zod.ZodAny) || isZodTypeExtends(schema, zod.ZodUnknown)) &&
      /^[{[]/.test(value)
    ) {
      try {
        return JSON.parse(value);
      } catch {}
    }
    return value;
  }

  protected decodeResult(table: Table, result: SqlResult): SqlResult {
    return mapSqlResult(result, (row) => {
      const entries = Object.entries(row);
      for (const entry of entries) {
        entry[1] = this.decodeSelectedValue(table.fields[entry[0]]?.schema, entry[1]);
      }
      return Object.fromEntries(entries);
    });
  }

  protected selectDialect?: SelectDialect;
  protected relationalSelect(query: SelectQuery, scalar = false, named = false) {
    if (!this.selectDialect) throw new Error("This adaptor does not support relational queries");
    return compileRelationalQuery(this, this.selectDialect, query, scalar, named);
  }
  protected buildRelationalSelectSql(query: SelectQuery, scalar: boolean, named = false): Statement {
    if (scalar && !isScalarSelect(query)) throw new Error("Scalar subqueries must select exactly one plain column");
    return raw(this.relationalSelect(query, scalar, named).sql);
  }
  protected async executeRelationalSelect(query: SelectQuery): Promise<SqlResult> {
    const compiled = this.relationalSelect(query);
    return mapSqlResult(await this.execute(raw(compiled.sql)), compiled.decode);
  }
  async executeQueryCount(query: SelectQuery): Promise<SqlResult> {
    const compiled = this.relationalSelect({
      ...query,
      includes: undefined,
      projection: undefined,
      orderBy: [],
      limit: undefined,
      offset: undefined,
    });
    const alias = this.quoteIdentifier("_zodbase_count");
    const columns = query.fields.map((field, i) =>
      field.key === "*"
        ? `COUNT(*) AS ${this.quoteIdentifier("_count")}`
        : `COUNT(${alias}.${this.quoteIdentifier(compiled.columns[i]!)}) AS ${this.quoteIdentifier(String(field.key))}`,
    );
    const result = await this.execute(raw(`SELECT ${columns.join(", ")} FROM (${compiled.sql}) AS ${alias}`));
    return mapSqlResult(result, (row) =>
      Object.fromEntries(Object.entries(row).map(([key, value]) => [key, Number(value)])),
    );
  }

  /**
   * Counts the query's rows, reading at most `max` of them: `COUNT(*)` over the query limited to
   * `max`, without its ordering, offset or includes. A result of `max` means "at least `max`".
   */
  async executeCappedCount(query: SelectQuery, max: number): Promise<number> {
    if (!Number.isSafeInteger(max) || max < 0) throw new Error("Invalid count limit");
    const compiled = this.relationalSelect({
      ...query,
      includes: undefined,
      projection: { _row: new SqlExpression(() => "1", zod.number()) },
      orderBy: [],
      limit: max,
      offset: undefined,
    });
    const count = this.quoteIdentifier("_count");
    const result = await this.execute(
      raw(`SELECT COUNT(*) AS ${count} FROM (${compiled.sql}) AS ${this.quoteIdentifier("_zodbase_count")}`),
    );
    return Number((result.first as Record<string, unknown> | undefined)?._count ?? 0);
  }

  //typeToSql: (type: zod.ZodType<any>) => string;
  //valuesSql: <T extends zod.ZodSchema>(values: zod.infer<T>, schema: T) => string;
  abstract executeSelect<TTable extends Table, TLimit extends number>(
    select: SelectQuery<Table, TLimit>,
  ): Promise<SqlResult<ValueOfTable<TTable>, TLimit>>;
  abstract execute(statement: Statement): Promise<SqlResult>;
  protected supportsSubqueryReturning = true;
  protected materializeRepeatedSubqueries = false;

  async compileStatement(statement: QueryStatement): Promise<Statement> {
    return statement.compile(this);
  }

  buildUpdateReturningSql(_table: Table, _values: Record<string, unknown>, _where: SelectCondition): Statement {
    throw new Error("This adaptor does not support UPDATE RETURNING in a writable CTE");
  }

  buildSelectSql(_select: SelectQuery, _scalar = false, _named = false): Statement {
    throw new Error("This adaptor does not support scalar subqueries");
  }

  async executeInsertSubqueries(
    table: Table,
    rows: Record<string, unknown>[],
    shouldReturn: boolean,
  ): Promise<SqlResult> {
    if (shouldReturn && !this.supportsSubqueryReturning)
      throw new Error(
        "This adaptor does not support subquery INSERT RETURNING; await the insert without selectMutated()",
      );
    const quote = (name: string) => this.quoteIdentifier(name);
    const keys = [...new Set(rows.flatMap((row) => Object.keys(row)))];
    const shared = new Map<string, { count: number; name: string }>();
    const compiled = new Map<unknown, string>();
    const names = new Set([String(table.id)]);
    for (const row of rows)
      for (const value of Object.values(row)) {
        if (!isSubquery(value)) continue;
        const query = value[SELECT_QUERY].query;
        names.add(String(query.table.id));
        const text = this.buildSelectSql(query, true)[TO_SQL_SYMBOL]();
        compiled.set(value, text);
        if (this.materializeRepeatedSubqueries && canShareSubquery(query, table)) {
          const entry = shared.get(text) ?? { count: 0, name: "" };
          entry.count++;
          shared.set(text, entry);
        }
      }
    let alias = 0;
    const common: Statement[] = [];
    for (const [text, entry] of shared) {
      if (entry.count < 2) {
        shared.delete(text);
        continue;
      }
      do {
        entry.name = `_zodbase_scalar_${alias++}`;
      } while (names.has(entry.name));
      names.add(entry.name);
      common.push(sql`${raw(quote(entry.name))} AS MATERIALIZED (SELECT (${raw(text)}) AS ${raw(quote("value"))})`);
    }
    const tuples = rows.map(
      (row) =>
        sql`(${join(
          keys.map((key) => {
            const value = row[key];
            return isSubquery(value)
              ? shared.has(compiled.get(value)!)
                ? sql`(SELECT ${raw(quote("value"))} FROM ${raw(quote(shared.get(compiled.get(value)!)!.name))})`
                : sql`(${raw(compiled.get(value)!)})`
              : raw(this.valueToSql(value, true));
          }),
          ", ",
        )})`,
    );
    const result = await this.execute(
      sql`INSERT INTO ${raw(quote(String(table.id)))} (${raw(keys.map(quote).join(", "))})${raw(shouldReturn ? this.insertReturningSql(table, "before-values") : "")} ${common.length ? sql`WITH ${join(common, ", ")} ` : raw("")}VALUES ${join(tuples, ", ")}${raw(shouldReturn ? this.insertReturningSql(table, "after-values") : "")}`,
    );
    return shouldReturn ? this.decodeResult(table, result) : { results: [], first: undefined };
  }

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
    /** The conflict target: the columns of the key the row is matched on, in order. */
    fields: readonly SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
  ): Promise<SqlResult<void, 0>>;
  /**
   * Upserts each row by `fields`, as `executeUpsert` would. Adaptors that can write the rows in one
   * statement override this; the default writes them one at a time.
   */
  async executeUpsertMany<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>[],
    fields: readonly SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
  ): Promise<SqlResult<void, 0>> {
    for (const rows of groupUpsertRows(
      values,
      fields.map((field) => String(field.key)),
    ))
      for (const row of rows) await this.executeUpsert(table, row, fields);
    return { results: [], first: undefined };
  }
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

  /** Characters other than `%` and `_` that are wildcards in this database's LIKE patterns. */
  protected likeSpecialCharacters = "";

  /**
   * A case-insensitive match against a pattern whose only wildcard is `*` (any run of characters).
   * Every other character, including LIKE's own `%` and `_`, matches itself.
   */
  buildPatternMatchSql(fieldSql: string, pattern: string, negate: boolean): Statement {
    return sql`${raw(fieldSql)} ${raw(negate ? "NOT LIKE" : "LIKE")} ${this.toLikePattern(pattern)} ESCAPE '!'`;
  }

  /** Case-insensitive substring match. Every character in the search term is literal. */
  buildContainsTextSql(fieldSql: string, text: string): Statement {
    const special = new Set(["!", "%", "_", ...this.likeSpecialCharacters]);
    const escaped = Array.from(text.toLowerCase(), (char) => (special.has(char) ? `!${char}` : char)).join("");
    return sql`LOWER(${raw(fieldSql)}) LIKE ${`%${escaped}%`} ESCAPE '!'`;
  }

  protected toLikePattern(pattern: string): string {
    const special = new Set(["!", "%", "_", ...this.likeSpecialCharacters]);
    return Array.from(pattern, (char) => (char === "*" ? "%" : special.has(char) ? `!${char}` : char)).join("");
  }

  /** Every table in the current schema, as `SchemaTableInfo` describes it. Read-only. */
  describeSchema(): Promise<SchemaTableInfo[]> {
    throw new Error("This adaptor does not support schema introspection");
  }

  abstract fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>>;
  abstract syncTableIndexes(table: Table): Promise<void>;
  /** The statement that creates one of a table's indexes. */
  abstract createIndexStatement(table: Table, index: TableIndex): Statement;

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

  async syncTableLocality(_table: Table): Promise<void> {}

  /** Statements a new table needs beyond its columns and indexes, such as where its rows live. */
  tableSettingStatements(_table: Table): Statement[] {
    return [];
  }

  createTable(table: Table, name?: string) {
    return this.execute(this.createTableStatement(table, name));
  }

  /** Whether `field` declares the table's primary key itself: the key is that one column. */
  protected isInlinePrimaryKey(table: Table, field: { key: unknown }): boolean {
    const keys = getPrimaryKey(table);
    return keys.length === 1 && keys[0] === String(field.key);
  }

  /** The table constraint naming a primary key of more than one column, in order. */
  protected primaryKeyConstraintSql(table: Table): string | undefined {
    const keys = getPrimaryKey(table);
    return keys.length > 1 ? `PRIMARY KEY (${keys.map((key) => this.quoteIdentifier(key)).join(", ")})` : undefined;
  }

  /** The statement that creates the table, doing nothing when it already exists. */
  createTableStatement(table: Table, name?: string): Statement {
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
                  this.isInlinePrimaryKey(table, field) ? "PRIMARY KEY" : "",
                  isZodRequired(schema) ? " NOT NULL" : "",
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
