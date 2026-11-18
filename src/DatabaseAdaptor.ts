import * as zod from "zod";
import { getMetaItem } from "zod-meta";
import { quoteIdentifier } from "./Escaping";
import {
  computed,
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
import { buildConditionSql, isFieldReference, isScalarSelect, SELECT_QUERY } from "./QueryBuilder";
import type { QueryStatement, WithClause } from "./QueryStatement";
import { compileRelationalQuery, type SelectDialect } from "./RelationalQuery";
import { divideSql, IncomingValue, SqlExpression } from "./SqlExpression";
import { type Statement, TO_SQL_SYMBOL } from "./Statement";
import { canShareSubquery, isSubquery } from "./Subquery";
import { getPrimaryKey, type LiveTableSchema, type SchemaTableInfo, type TableIndex, writableFieldKeys } from "./Table";

/**
 * The driver an adaptor runs statements with: one ready to use, or, with `lazy: true`, a function
 * that opens one, called once before the adaptor's first statement (and again after it fails), so a
 * database that's never queried never connects.
 */
export type DatabaseAdaptorDriver<TDriver = any> =
  | { driver: TDriver; lazy?: false }
  | { driver: () => TDriver | Promise<TDriver>; lazy: true };

export type DatabaseAdaptorOptions<TDriver = any> = DatabaseAdaptorDriver<TDriver> & {
  debug?: boolean;
  events?: DatabaseEvents;
};

export interface PossiblySelectedResult<TValue = any, TLimit extends number = number>
  extends SqlResult<TValue, TLimit> {
  selected: boolean;
}

/** What an insert does with a row that conflicts with an existing one on a unique key (`onConflict`). */
export interface Conflict {
  /** The unique key's columns, in order. */
  readonly fields: readonly SingleFieldBinding[];
  /** The condition of the partial unique index the key is, which names that index as the target. */
  readonly where?: SelectCondition;
  readonly action: "nothing" | "update";
  /**
   * What an update sets, parsed: `incoming(field)` in it is the value the row brought. Without it,
   * every column the row gives but the key's takes the row's value.
   */
  readonly set?: Record<string, unknown>;
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

/** `condition` with each `incoming(field)` in it replaced by `replace(field)`. */
export const resolveIncoming = (
  condition: SelectCondition,
  replace: (field: IncomingValue["field"]) => unknown,
): SelectCondition => {
  if ("conditions" in condition)
    return { ...condition, conditions: condition.conditions.map((child) => child && resolveIncoming(child, replace)) };
  if ("exists" in condition) return condition;
  return condition.value instanceof IncomingValue ? { ...condition, value: replace(condition.value.field) } : condition;
};

/** The fields `incoming(…)` names in `condition`. */
/** The adaptor, once its driver is open: what anything that runs a statement awaits first. */
export const connected = async <TAdaptor extends DatabaseAdaptor>(adaptor: TAdaptor): Promise<TAdaptor> => {
  await adaptor.connect();
  return adaptor;
};

export const incomingFields = (condition: SelectCondition | undefined): string[] => {
  if (!condition || "exists" in condition) return [];
  if ("conditions" in condition) return condition.conditions.flatMap((child) => incomingFields(child));
  return condition.value instanceof IncomingValue ? [String(condition.value.field.key)] : [];
};

/**
 * The condition updating `row` in an update of several rows: its key, matched on each of `fields`,
 * and `where`, if given, with `incoming(…)` taking the row's values.
 */
export const keyCondition = (
  fields: readonly SingleFieldBinding<any, any>[],
  row: Record<string, unknown>,
  where?: SelectCondition,
) =>
  [
    ...fields.map((field) => field.equals(row[String(field.key)] as any) as SelectCondition),
    ...(where ? [resolveIncoming(where, (field) => row[String(field.key)])] : []),
  ].reduce((all, condition) => all.and(condition));

export default abstract class DatabaseAdaptor<TDriver = any> {
  private transactionQueue: Promise<void> = Promise.resolve();

  /** The driver statements run with: given, or opened from a lazy one. */
  private openDriver: TDriver | undefined;
  private opening: Promise<TDriver> | undefined;

  constructor(protected readonly options: DatabaseAdaptorOptions<TDriver>) {
    if (options.lazy) {
      if (typeof options.driver !== "function")
        throw new Error("[zodbase] A lazy driver must be a function that opens the driver");
    } else this.openDriver = options.driver;
  }

  /**
   * Opens a lazy driver, once: statements that start while it opens wait for the same driver, and
   * one that fails to open leaves the next to try again. A driver given ready is already open.
   */
  async connect(): Promise<void> {
    if (this.openDriver !== undefined || !this.options.lazy) return;
    const open = this.options.driver;
    this.opening ??= Promise.resolve()
      .then(open)
      .then(
        (driver) => (this.openDriver = driver),
        (error: unknown) => {
          this.opening = undefined;
          throw error;
        },
      );
    await this.opening;
  }

  /** This adaptor's options for one running on `driver`, already open: a transaction's client, say. */
  protected optionsFor<TOther>(driver: TOther): DatabaseAdaptorOptions<TOther> {
    const { driver: _driver, lazy: _lazy, ...rest } = this.options;
    return { ...rest, driver };
  }

  protected get driver(): TDriver {
    if (this.openDriver === undefined)
      throw new Error("[zodbase] The database's lazy driver isn't open: a statement ran before connect()");
    return this.openDriver;
  }

  quoteIdentifier(value: string): string {
    return quoteIdentifier(value);
  }

  valueToSql(value: unknown, nested = false): string {
    if (value instanceof SqlExpression) return value.toSql(this);
    // A column as a value (`update(T, { at: T.$at })`), quoted as this database quotes names.
    if (isFieldReference(value))
      return `${this.quoteIdentifier(String(value.table.id))}.${this.quoteIdentifier(String(value.key))}`;
    return valueToSql(value, nested);
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
    if (!query.with) return compileRelationalQuery(this, this.selectDialect, query, scalar, named);
    const compiled = compileRelationalQuery(this, this.selectDialect, { ...query, with: undefined }, scalar, named);
    return { ...compiled, sql: this.prefixWith(query.with, raw(compiled.sql))[TO_SQL_SYMBOL]() };
  }

  /** Whether a `WITH` clause may hold inserts, updates and deletes: PostgreSQL and CockroachDB only. */
  protected supportsWritesInWith = false;
  /** The keyword a recursive `WITH` clause takes; SQL Server's takes none. */
  protected recursiveWithKeyword = "RECURSIVE ";

  /** `statement` starting with `clause` (`WITH name AS (query), …`). */
  prefixWith(clause: WithClause, statement: Statement): Statement {
    if (!clause.entries.length) return statement;
    if (clause.entries.some((entry) => entry.write) && !this.supportsWritesInWith)
      throw new Error("[zodbase] Only PostgreSQL and CockroachDB run inserts, updates and deletes in a WITH clause");
    const definitions = clause.entries.map(
      ({ table, query }) => sql`${raw(this.quoteIdentifier(String(table.id)))} AS (${query.compile(this)})`,
    );
    return sql`WITH ${raw(clause.recursive ? this.recursiveWithKeyword : "")}${join(definitions, ", ")} ${statement}`;
  }

  /**
   * How `incoming(field)` compiles where it has a value: set on an adaptor `withIncoming` gives an
   * upsert's update, and absent elsewhere, where building it throws.
   */
  incomingSql?: (column: string) => string;

  /** This adaptor with `incoming(field)` compiling to `sqlFor(column)`. */
  protected withIncoming(sqlFor: (column: string) => string): this {
    return Object.create(this, { incomingSql: { value: sqlFor } });
  }

  /**
   * The assignments a conflict's update makes: its `set`, or each column the row gives but the key's,
   * which the existing row already has. Setting a key column, even to its own value, is a key change
   * to CockroachDB, which then checks the key is unique in every region of a regional-by-row table
   * and gives up the auto-commit fast path. A row of only key columns sets its first, so the upsert
   * still returns the row. `incomingSql` is the inserted row's value of a column.
   */
  protected conflictAssignments(
    conflict: Conflict,
    columns: readonly string[],
    incomingSql: (column: string) => string,
  ): string {
    const quote = (name: string) => this.quoteIdentifier(name);
    if (conflict.set) {
      const scoped = this.withIncoming(incomingSql);
      const assignments = Object.entries(conflict.set)
        .filter(([, value]) => value !== undefined)
        .map(([column, value]) => `${quote(column)} = ${scoped.valueToSql(value, true)}`);
      if (!assignments.length) throw new Error("[zodbase] A conflict's update needs values to set");
      return assignments.join(", ");
    }
    const keys = conflict.fields.map((field) => String(field.key));
    const assigned = columns.filter((column) => !keys.includes(column));
    return (assigned.length ? assigned : keys.slice(0, 1))
      .map((column) => `${quote(column)} = ${incomingSql(column)}`)
      .join(", ");
  }

  /**
   * `ON CONFLICT (key) [WHERE condition] DO NOTHING | DO UPDATE SET …`, as PostgreSQL, CockroachDB
   * and SQLite write it, for rows that give `columns`.
   */
  protected onConflictSql(conflict: Conflict, columns: readonly string[]): string {
    const quote = (name: string) => this.quoteIdentifier(name);
    const target = `(${conflict.fields.map((field) => quote(String(field.key))).join(", ")})${
      conflict.where
        ? ` WHERE ${buildConditionSql(this, conflict.where, { includeTable: false })[TO_SQL_SYMBOL]()}`
        : ""
    }`;
    if (conflict.action === "nothing") return ` ON CONFLICT ${target} DO NOTHING`;
    return ` ON CONFLICT ${target} DO UPDATE SET ${this.conflictAssignments(conflict, columns, (column) => `excluded.${quote(column)}`)}`;
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
  /** Whether an update can return the rows it changed (MySQL's has no RETURNING). */
  readonly updateReturnsRows: boolean = true;
  protected materializeRepeatedSubqueries = false;

  async compileStatement(statement: QueryStatement): Promise<Statement> {
    return statement.compile(this);
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
  /** Inserts the row, or updates the one it conflicts with as `conflict` (an update) says. */
  abstract executeUpsert<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    conflict: Conflict,
  ): Promise<SqlResult<void, 0>>;
  /**
   * Upserts each row, as `executeUpsert` would. Adaptors that can write the rows in one statement
   * override this; the default writes them one at a time.
   */
  async executeUpsertMany<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>[],
    conflict: Conflict,
  ): Promise<SqlResult<void, 0>> {
    for (const rows of groupUpsertRows(
      values,
      conflict.fields.map((field) => String(field.key)),
    ))
      for (const row of rows) await this.executeUpsert(table, row, conflict);
    return { results: [], first: undefined };
  }
  abstract executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(
    table: TTable,
    values: TValue[],
    fields: readonly SingleFieldBinding<TValue, TKey>[],
    where?: SelectCondition,
  ): Promise<SqlResult<void, 0>>;
  abstract executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
    where: SelectCondition<ValueOfTable<TTable>> | undefined,
  ): Promise<SqlResult<Record<TKey, number>, 1>>;
  abstract executeDelete<TTable extends Table>(
    table: TTable,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>>;

  /** A result's rows as the table's values, as a select's are decoded. */
  decode(table: Table, result: SqlResult): SqlResult {
    return this.decodeResult(table, result);
  }

  /** The rows' values as SQL tuples, every row with `columns` in order (a missing one is NULL). */
  protected valueTuples(rows: readonly Record<string, unknown>[], columns: readonly string[]): string {
    return rows.map((row) => `(${columns.map((key) => this.valueToSql(row[key], true)).join(", ")})`).join(", ");
  }

  /**
   * Inserts `rows`, skipping any that conflict on `fields` (a unique key), and returns the rows it
   * inserted when asked. MySQL and MariaDB skip with `INSERT IGNORE`, which can't return them.
   */
  async executeInsertIgnoring<TTable extends Table>(
    table: TTable,
    rows: InputOfTable<TTable>[],
    conflict: Conflict,
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>>> {
    if (!rows.length) return { results: [], first: undefined };
    const columns = writableFieldKeys(table);
    const quote = (name: string) => this.quoteIdentifier(name);
    const into = `${quote(String(table.id))} (${columns.map(quote).join(", ")})`;
    const values = this.valueTuples(rows as Record<string, unknown>[], columns);
    if (this.selectDialect === "mysql" || this.selectDialect === "mariadb") {
      if (conflict.where) throw new Error("[zodbase] MySQL can't name a partial index as a conflict target");
      if (shouldReturn) throw new Error("[zodbase] MySQL can't return the rows an insert that skips conflicts wrote");
      await this.execute(raw(`INSERT IGNORE INTO ${into} VALUES ${values}`));
      return { results: [], first: undefined };
    }
    if (this.selectDialect === "mssql") throw new Error("[zodbase] SQL Server can't skip conflicting inserts");
    const result = await this.execute(
      raw(
        `INSERT INTO ${into} VALUES ${values}${this.onConflictSql(conflict, columns)}${shouldReturn ? this.insertReturningSql(table, "after-values") : ""}`,
      ),
    );
    return shouldReturn ? (this.decodeResult(table, result) as any) : { results: [], first: undefined };
  }

  /**
   * `INSERT INTO table (columns) <select>`, returning the inserted rows when asked: the select's
   * projection gives the columns' values in order.
   */
  buildInsertSelectSql(
    table: Table,
    columns: readonly string[],
    select: Statement,
    shouldReturn: boolean,
    conflict?: Conflict,
  ): Statement {
    const quote = (name: string) => this.quoteIdentifier(name);
    if (conflict && this.selectDialect === "mssql")
      throw new Error("[zodbase] SQL Server can't handle conflicts in an insert from a select");
    return sql`INSERT INTO ${raw(quote(String(table.id)))} (${raw(columns.map(quote).join(", "))})${raw(
      shouldReturn ? this.insertReturningSql(table, "before-values") : "",
    )} ${this.insertSelectSource(select, conflict)}${raw(conflict ? this.onConflictSql(conflict, columns) : "")}${raw(
      shouldReturn ? this.insertReturningSql(table, "after-values") : "",
    )}`;
  }

  /**
   * `INSERT INTO table (columns) SELECT <row> [WHERE condition] UNION ALL …`: each row's values,
   * only where `condition` holds when given. A value may be a scalar subquery. `condition` stands on
   * its own (`exists(…)`, a subquery): no table is in scope for it.
   */
  buildConditionalInsertSql(
    table: Table,
    rows: readonly Record<string, unknown>[],
    condition: SelectCondition | undefined,
    shouldReturn: boolean,
    conflict?: Conflict,
  ): Statement {
    const columns = writableFieldKeys(table);
    const fields = table.fields as Record<string, { schema: zod.ZodType }>;
    const where = condition ? ` WHERE ${buildConditionSql(this, condition, true)[TO_SQL_SYMBOL]()}` : "";
    const selects = rows.map(
      (row) =>
        `SELECT ${columns
          .map((column) => {
            const value = row[column];
            const literal = isSubquery(value)
              ? `(${this.buildSelectSql(value[SELECT_QUERY].query, true)[TO_SQL_SYMBOL]()})`
              : this.valueToSql(value, true);
            return `${this.insertSelectValueSql(literal, fields[column]!.schema)} AS ${this.quoteIdentifier(column)}`;
          })
          .join(", ")}${this.tablelessSelectSql}${where}`,
    );
    return this.buildInsertSelectSql(table, columns, raw(selects.join(" UNION ALL ")), shouldReturn, conflict);
  }

  /**
   * A derived table of `rows` with `table`'s columns (`values`), each value cast to its column's
   * type: `(SELECT … UNION ALL SELECT …)`, which every dialect reads, unlike VALUES with column names.
   */
  rowsSourceSql(table: Table, rows: readonly Record<string, unknown>[]): string {
    const fields = Object.entries(table.fields as Record<string, { schema: zod.ZodType }>);
    const select = (row?: Record<string, unknown>) =>
      `SELECT ${fields
        .map(
          ([key, field]) =>
            `${this.insertSelectValueSql(row ? this.valueToSql(row[key], true) : "NULL", field.schema)} AS ${this.quoteIdentifier(key)}`,
        )
        .join(", ")}`;
    return `(${rows.length ? rows.map((row) => select(row)).join(" UNION ALL ") : `${select()}${this.tablelessSelectSql} WHERE 1 = 0`})`;
  }

  /** What a select of no table takes before a WHERE: MySQL's `FROM DUAL`. */
  protected tablelessSelectSql = "";

  /**
   * A value an insert selects, cast to its column's type: unlike an insert's values, a select's
   * literals take no type from the column they go to.
   */
  protected insertSelectValueSql(value: string, schema: zod.ZodType): string {
    const type = this.typeToSql(schema);
    return type === "NULL" ? value : `CAST(${value} AS ${type})`;
  }

  /** The select an insert takes its rows from, as the dialect reads it before a conflict clause. */
  protected insertSelectSource(select: Statement, _conflict?: Conflict): Statement {
    return select;
  }

  /**
   * The statement an insert runs, for adaptors that can run it beside others in one round trip or
   * inside a writable CTE (`executeTogether`): `undefined` elsewhere. Every row sets the table's
   * columns, in order.
   */
  buildInsertStatement(
    _table: Table,
    _rows: readonly Record<string, unknown>[],
    _options: {
      returning: boolean;
      conflict?: Conflict;
    },
  ): Statement | undefined {
    return undefined;
  }

  /** The statement an update runs, as `buildInsertStatement` gives an insert's. */
  buildUpdateStatement(
    _table: Table,
    _values: Record<string, unknown>,
    _where: SelectCondition,
    _returning: boolean,
  ): Statement | undefined {
    return undefined;
  }

  /** The statement a delete runs, as `buildInsertStatement` gives an insert's. */
  buildDeleteStatement(_table: Table, _where: SelectCondition, _returning = false): Statement | undefined {
    return undefined;
  }

  /**
   * Runs the statements in one round trip as one transaction, returning each one's rows, where the
   * database can (`undefined` elsewhere, and the caller runs them in a transaction instead).
   */
  async executeTogether(_statements: readonly Statement[]): Promise<SqlResult[] | undefined> {
    return undefined;
  }

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

  /**
   * One ORDER BY term. NULL sorts as the smallest value, first ascending and last descending, as
   * SQLite, MySQL and CockroachDB order it.
   */
  orderTermSql(columnSql: string, _field: SingleFieldBinding, direction: "ASC" | "DESC"): string {
    return `${columnSql} ${direction}`;
  }

  /** A statement's text, as this database is sent it. */
  protected sqlText(statement: Statement): string {
    return statement[TO_SQL_SYMBOL]();
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

  /**
   * Every table in the current schema by name, with its columns and indexes, read in a few round
   * trips for all tables at once; undefined when the adaptor can only read them table by table, so
   * `syncTables` syncs each table on its own. Read-only.
   */
  async fetchLiveSchema(): Promise<Map<string, LiveTableSchema> | undefined> {
    return undefined;
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

  /**
   * How a computed column (`computed`) declares its expression, after its type: a stored generated
   * column. Empty for any other column.
   */
  protected computedColumnSql(_table: Table, field: { key: unknown; schema: zod.ZodType }): string {
    const options = getMetaItem(field.schema, computed)?.data;
    return options ? `GENERATED ALWAYS AS (${options.sql((key) => this.quoteIdentifier(key))}) STORED` : "";
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
                  this.computedColumnSql(table, field),
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
