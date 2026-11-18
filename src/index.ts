import { assertSelectArguments, type InsertValues, isFieldReference, SELECT_QUERY } from "./QueryBuilder";
import {
  COMPILE,
  type Compilable,
  extendWithClause,
  isCompilable,
  QueryStatement,
  toQueryStatement,
  type WithClause,
} from "./QueryStatement";
import { isRelationalQuery } from "./RelationalQuery";
import { SqlExpression } from "./SqlExpression";
import { snapshotQuery } from "./Subquery";
import {
  createDelete,
  createInsert,
  createInsertSelect,
  createUpdate,
  createUpdateRows,
  type DeleteWrite,
  type InsertSelectWrite,
  type InsertWrite,
  isWrite,
  runBatch,
  type SubqueryInsertWrite,
  type UpdateRows,
  type UpdateWrite,
  type Write,
  type WriteResult,
} from "./Writes";

export type { InsertValues, ScalarSubquery } from "./QueryBuilder";

import * as zod from "zod";
import { getMetaItem } from "zod-meta";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import { connected } from "./DatabaseAdaptor";
import { escapeSqlValue } from "./Escaping";
import { toLazyPromise } from "./LazyPromise";
import { computed, foreignKey } from "./MetaTypes";
import type {
  BindingKeys,
  FieldBinding,
  InputOfTable,
  SelectCondition,
  Selection,
  SelectionValue,
  SelectQuery,
  SelectQueryBuilder,
  SingleFieldBinding,
  SqlResult,
  SqlResultTimings,
  StringKeys,
  UpdateValues,
  ValueOfTable,
} from "./QueryBuilder";
import { type Statement, TO_SQL_SYMBOL, type ToSql } from "./Statement";
import {
  getPrimaryKey,
  type LiveTableSchema,
  type SchemaDrift,
  type SchemaTableInfo,
  type Table,
  type TableColumnInfo,
  type TableSyncChanges,
} from "./Table";
import { isZodRequired, isZodTypeExtends } from "./ZodUtils";

export * from "./index.common";
export * from "./ZodUtils";
export { TO_SQL_SYMBOL };

export type OrderDirection = "ASC" | "DESC";

const IS_REACT_NATIVE = typeof navigator !== "undefined" && (navigator as any).product === "ReactNative";

if (typeof (globalThis as any).window !== "undefined" && !IS_REACT_NATIVE) {
  throw new Error("[zodbase] This package is not intended for browser usage");
}

export type { InputOfTable } from "./QueryBuilder";

export const valueToSql = (value: any, nested?: boolean): string => {
  if (value?.[TO_SQL_SYMBOL]) {
    return value[TO_SQL_SYMBOL]();
  }

  if (value instanceof Date) return escapeSqlValue(value.toISOString().replace("T", " ").replace("Z", ""));

  if (typeof value === "number") {
    return value.toString();
  }

  if (value === "") {
    return "''";
  }

  if (Array.isArray(value) && !nested) {
    return `(${value.map((value) => valueToSql(value, true)).join(", ")})`;
  }

  if (typeof value === "undefined" || value === null) {
    return "null";
  }
  if (typeof value === "object") {
    const objectString = escapeSqlValue(JSON.stringify(value));
    return nested ? objectString : `(${objectString})`;
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  return escapeSqlValue(value?.toString());
};

/*export const upsertSql = <T extends zod.ZodObject<any>>(schema: T, entity: zod.input<T>): string => {
  const tableMeta = getValidMeta(schema, table);
  if (!tableMeta) {
    throw new Error("Table name is required");
  }
  const primaryKeyName = findFieldMeta(schema, primaryKey)?.key;
  const safeEntity = schema.parse(entity);
  return `INSERT INTO ${tableMeta.options.name} (${Object.keys(safeEntity).join(", ")}) VALUES (${Object.values(
    safeEntity,
  ).map((value) => {
    if (typeof value === "string") {
      return `'${value}'`;
    }
    return value;
  })}) ON CONFLICT(${primaryKeyName}) DO UPDATE SET ${SQLITE_ADAPTER.valuesSql(safeEntity, schema)}`;
};

export const updateSql = <T extends zod.ZodObject<any>>(schema: T, entity: Partial<zod.input<T>>): string => {
  const tableMeta = getValidMeta(schema, table);
  if (!tableMeta) {
    throw new Error("Table name is required");
  }
  const primaryKeyName = findFieldMeta(schema, primaryKey)?.key;
  const safeEntity = schema.partial().parse(entity);
  return `UPDATE ${tableMeta.options.name} SET ${SQLITE_ADAPTER.valuesSql(
    safeEntity,
    schema,
  )} WHERE ${primaryKeyName} = '${safeEntity[primaryKeyName]}'`;
};*/

export interface ExecuteStatementEvent {
  success: boolean;
  timings: SqlResultTimings;
  sql: string;
}

export interface DatabaseEvents {
  onExecuteStatement?: (event: ExecuteStatementEvent) => void;
}

export interface DatabaseOptions {
  /** Connects lazily when its driver is given as `{ driver: () => …, lazy: true }`. */
  adaptor: DatabaseAdaptor;
  events?: DatabaseEvents;
  debug?: boolean;
}

type ResolvedDatabaseOptions = DatabaseOptions;

interface BaseFieldModification<M> {
  type: M;
}

export interface AddConstraint extends BaseFieldModification<"add-constraint"> {
  constraint: string;
}

export interface RemoveConstraint extends BaseFieldModification<"remove-constraint"> {
  constraint: string;
}

export interface AddForeignKey extends BaseFieldModification<"add-foreign-key"> {
  foreignKey: import("./Table").TableForeignKeyInfo;
}

export interface RemoveForeignKey extends BaseFieldModification<"remove-foreign-key"> {
  foreignKey: import("./Table").TableForeignKeyInfo;
}

export type FieldModification =
  | AddConstraint
  | RemoveConstraint
  | AddForeignKey
  | RemoveForeignKey
  | BaseFieldModification<"widen-number" | "widen-date">;

export type FieldDiffType = "added" | "removed" | "modified";

export interface FieldDiff {
  key: string;
  field?: SingleFieldBinding;
  type: FieldDiffType;
  modifications?: FieldModification[];
}

export interface TableDiff {
  fields: FieldDiff[];
  /**
   * The primary key's columns in order, when the table's key must change to them: a key of more
   * than one column, or a different key. Adaptors apply it after adding columns.
   */
  primaryKey?: string[];
}

const getFieldBindingsByKeys = <TTable extends Table>(
  table: TTable,
  keys: BindingKeys<ValueOfTable<TTable>>[],
): FieldBinding<ValueOfTable<TTable>>[] => {
  return keys.map<FieldBinding<ValueOfTable<TTable>>>((key) => {
    if (key === "*") {
      return {
        table: table,
        key: "*",
      } as any;
    }
    return table.fields[key as string];
  });
};

export const join = (items: ToSql[], separator: string): ToSql => {
  return {
    [TO_SQL_SYMBOL]: () => items.map((item) => item[TO_SQL_SYMBOL]()).join(separator),
  };
};

// tag function for creating sql strings
export function sql(strings: TemplateStringsArray, ...values: any[]): Statement {
  let result = "";
  for (let i = 0; i < strings.length; i++) {
    result += strings[i];
    if (i < values.length) {
      const value = values[i];
      result += valueToSql(value);
    }
  }
  return { [TO_SQL_SYMBOL]: () => result };
}

export const isToSql = (value: any): value is ToSql => {
  return typeof value?.[TO_SQL_SYMBOL] === "function";
};

export function raw(value: string | string[] | Statement | Statement[]): Statement {
  return {
    [TO_SQL_SYMBOL]: () => {
      if (isToSql(value)) {
        return value[TO_SQL_SYMBOL]();
      }

      if (Array.isArray(value)) {
        return value
          .map((subValue) => {
            return isToSql(subValue) ? subValue[TO_SQL_SYMBOL]() : subValue;
          })
          .join(", ");
      }

      return value;
    },
  };
}

const createSelectQueryBuilder = <TTable extends Table, TKey extends BindingKeys<ValueOfTable<TTable>>>(
  query: SelectQuery<TTable>,
  adaptor: DatabaseAdaptor,
): SelectQueryBuilder<
  TTable,
  "*" extends TKey ? ValueOfTable<TTable> : Pick<ValueOfTable<TTable>, Exclude<TKey, "*">>,
  number
> => {
  const builder = {
    [SELECT_QUERY]: { query },
    table: query.table,

    clone() {
      return createSelectQueryBuilder(snapshotQuery(query), adaptor);
    },

    where(condition: SelectCondition<ValueOfTable<TTable>>) {
      if (query.where) {
        query.where = query.where.and(condition);
      } else {
        query.where = condition;
      }
      return this;
    },
    limit<TLimit extends number>(limit: TLimit) {
      query.limit = limit;
      return this;
    },
    offset(offset: number) {
      query.offset = offset;
      return this;
    },
    one() {
      return this.limit(1);
    },
    orderBy(field: SingleFieldBinding<ValueOfTable<TTable>>, direction: OrderDirection) {
      query.orderBy.push({
        field: field,
        direction,
      });
      return this;
    },
    forceIndex(index: string) {
      query.index = index;
      return this;
    },
    leftJoin(table: Table, on: SelectCondition) {
      (query.joins ??= []).push({ type: "LEFT", table, on });
      return this;
    },
    innerJoin(table: Table, on: SelectCondition) {
      (query.joins ??= []).push({ type: "INNER", table, on });
      return this;
    },
    crossJoin(table: Table) {
      (query.joins ??= []).push({ type: "CROSS", table });
      return this;
    },
    groupBy(...fields: SingleFieldBinding[]) {
      (query.groupBy ??= []).push(...fields);
      return this;
    },
    include(includes: Record<string, { [SELECT_QUERY]: { query: SelectQuery } }>) {
      query.includes = {
        ...query.includes,
        ...Object.fromEntries(
          Object.entries(includes).map(([key, value]) => {
            if (!value?.[SELECT_QUERY]) throw new Error("Includes must be SELECT query builders");
            return [key, snapshotQuery(value[SELECT_QUERY].query)];
          }),
        ),
      };
      return this;
    },

    async execute() {
      assertSelectArguments(query);
      return (await connected(adaptor)).executeSelect(query);
    },
    /**
     * The number of rows the query matches, ignoring its ordering, limit and offset. With `upTo`, it
     * reads at most that many, so counting a large match stays cheap: a result of `upTo` means "at
     * least `upTo`".
     */
    async count(options: { upTo?: number } = {}): Promise<number> {
      await adaptor.connect();
      if (options.upTo !== undefined) return adaptor.executeCappedCount(query, options.upTo);
      const all = [{ table: query.table, key: "*" }] as any;
      const result = isRelationalQuery(query)
        ? await adaptor.executeQueryCount({ ...query, fields: all })
        : await adaptor.executeCount(query.table, all, query.where);
      return Number((result.first as { _count?: unknown } | undefined)?._count ?? 0);
    },
  };

  return toLazyPromise(async () => {
    assertSelectArguments(query);
    return (await connected(adaptor)).executeSelect(query);
  }, builder) as any;
};

/** Loads (`Database.load`) of one table's rows by one field, under one condition, waiting to run together. */
interface PendingLoads {
  readonly field: SingleFieldBinding;
  readonly where: SelectCondition | undefined;
  readonly values: Map<
    string,
    { value: unknown; waiting: { resolve(row: unknown): void; reject(error: unknown): void }[] }
  >;
}

/** The most values one statement of loads reads by. */
const LOAD_CHUNK_SIZE = 500;

/**
 * A text two conditions share exactly when they compile to the same SQL, or undefined where that
 * can't be told from the condition alone (subqueries, field references, expressions).
 */
const conditionKey = (condition: SelectCondition | undefined): string | undefined => {
  if (!condition) return "";
  if ("exists" in condition) return undefined;
  if ("conditions" in condition) {
    const parts = condition.conditions.map((child) => (child ? conditionKey(child) : ""));
    return parts.some((part) => part === undefined) ? undefined : `${condition.type}(${parts.join(",")})`;
  }
  const { value } = condition;
  if (isFieldReference(value) || isCompilable(value) || value instanceof SqlExpression) return undefined;
  let text: string | undefined;
  try {
    text = JSON.stringify(value);
  } catch {
    return undefined;
  }
  return `${String(condition.field.table.id)}.${String(condition.field.key)} ${condition.operator} ${text}`;
};

/** `adaptor`, reporting each of its statements that fails to `onFailure`. */
const trackFailures = (adaptor: DatabaseAdaptor, onFailure: (error: unknown) => void): DatabaseAdaptor =>
  new Proxy(adaptor, {
    get(target, property) {
      const value = Reflect.get(target, property, target);
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        const result = value.apply(target, args);
        if (typeof result?.then === "function") result.then(undefined, onFailure);
        return result;
      };
    },
  });

/** What a transaction's database runs once the transaction has ended: nothing. */
const ENDED_TRANSACTION = new Proxy({} as DatabaseAdaptor, {
  get: () => () => {
    throw new Error("[zodbase] This transaction has ended: run its statements inside its callback");
  },
});

export class Database {
  private readonly options: ResolvedDatabaseOptions;
  private readonly loads = new Map<string, PendingLoads>();

  constructor(
    options: DatabaseOptions,
    private readonly transactionScoped = false,
    /** The named queries every statement it builds starts with (`with`). */
    private readonly withClause?: WithClause,
  ) {
    this.options = options;
  }

  /** The SQL any builder or statement compiles to for this database. */
  compile(statement: Compilable): Promise<Statement> {
    if (isWrite(statement)) {
      const write = statement;
      return Promise.resolve(
        this.options.adaptor.compileStatement(new QueryStatement((adaptor) => write[COMPILE](adaptor))),
      );
    }
    return this.options.adaptor.compileStatement(toQueryStatement(statement));
  }

  async execute(statement: Statement | Compilable) {
    const adaptor = await this.connected();
    return adaptor.execute(isCompilable(statement) ? await this.compile(statement) : statement);
  }

  /**
   * Runs the writes atomically, in the fewest round trips the database allows: one on PostgreSQL
   * and CockroachDB (a query of every statement, which they run as one transaction), else a
   * transaction. Each result is what awaiting that write alone gives. No write may depend on
   * another's rows: every statement sees the database as it was before the batch.
   */
  async batch<const TWrites extends readonly Write<any>[]>(
    writes: TWrites,
  ): Promise<{ -readonly [K in keyof TWrites]: WriteResult<TWrites[K]> }> {
    for (const write of writes) if (!isWrite(write)) throw new Error("[zodbase] A batch takes only write builders");
    return (await runBatch(await this.connected(), writes)) as any;
  }

  /** The adaptor, its driver open: what each statement this database runs awaits first. */
  private connected(): Promise<DatabaseAdaptor> {
    return connected(this.options.adaptor);
  }

  /** Opens the database's lazy driver now rather than with its first statement. */
  async connect(): Promise<void> {
    await this.options.adaptor.connect();
  }

  /**
   * The row whose `field` (a unique one, such as the primary key) is `value`, or undefined. Loads of
   * the same table and field under the same `where` that this database starts in the same turn of
   * the event loop read their rows together, in one `WHERE field IN (…)` statement rather than one
   * each; a transaction's loads never share one with others. A failed statement rejects every load
   * it held.
   */
  load<TValue, TKey extends StringKeys<TValue>>(
    field: SingleFieldBinding<TValue, TKey>,
    value: TValue[TKey],
    options: { where?: SelectCondition<TValue> } = {},
  ): Promise<TValue | undefined> {
    const condition = conditionKey(options.where);
    const valueKey = JSON.stringify(value);
    const read = (values: unknown[]) => {
      const matching = (field as SingleFieldBinding).in(values as never) as SelectCondition;
      return this.select(field.table).where(options.where ? matching.and(options.where) : matching);
    };
    // A condition that can't be compared with others reads on its own.
    if (condition === undefined || valueKey === undefined)
      return Promise.resolve(read([value]).one()).then(({ first }) => first as TValue | undefined);
    const key = `${String(field.table.id)}\u0000${String(field.key)}\u0000${condition}`;
    let pending = this.loads.get(key);
    if (!pending) {
      const created: PendingLoads = { field, where: options.where, values: new Map() };
      pending = created;
      this.loads.set(key, created);
      setTimeout(() => {
        this.loads.delete(key);
        void this.runLoads(created, read);
      }, 0);
    }
    const entry = pending.values.get(valueKey) ?? { value, waiting: [] };
    pending.values.set(valueKey, entry);
    return new Promise((resolve, reject) => entry.waiting.push({ resolve: resolve as (row: unknown) => void, reject }));
  }

  private async runLoads(
    pending: PendingLoads,
    read: (values: unknown[]) => Promise<SqlResult<Record<string, unknown>>>,
  ): Promise<void> {
    const entries = [...pending.values.values()];
    try {
      const chunks: (typeof entries)[] = [];
      for (let index = 0; index < entries.length; index += LOAD_CHUNK_SIZE)
        chunks.push(entries.slice(index, index + LOAD_CHUNK_SIZE));
      const results = await Promise.all(chunks.map((chunk) => read(chunk.map((entry) => entry.value))));
      const rows = new Map<string, unknown>();
      for (const { results: found } of results)
        for (const row of found) rows.set(JSON.stringify(row[String(pending.field.key)]), row);
      for (const entry of entries)
        for (const waiting of entry.waiting) waiting.resolve(rows.get(JSON.stringify(entry.value)));
    } catch (error) {
      for (const entry of entries) for (const waiting of entry.waiting) waiting.reject(error);
    }
  }

  /**
   * A database whose selects, inserts, updates and deletes start with `WITH table AS (query)`: they
   * read the query's rows through `table`, whose fields name its columns (a `createTable` of the
   * query's shape, never synced). Chain `with` for several. On PostgreSQL and CockroachDB the query
   * may be an insert, update or delete with `returning`, and the statement after it sees what it
   * returned; elsewhere a WITH clause holds selects only, and a write runs after one on PostgreSQL
   * and CockroachDB only.
   */
  with(table: Table, query: Compilable): Database {
    return this.scoped(extendWithClause(this.withClause, table, toQueryStatement(query), isWrite(query), false));
  }

  /** `with`, where the query may read its own table: `WITH RECURSIVE`, as a union of a seed and a step. */
  withRecursive(table: Table, query: Compilable): Database {
    return this.scoped(extendWithClause(this.withClause, table, toQueryStatement(query), isWrite(query), true));
  }

  private scoped(clause: WithClause): Database {
    return new Database(this.options, this.transactionScoped, clause);
  }

  transaction<TResult>(callback: (transaction: Database) => Promise<TResult>): Promise<TResult> {
    if (this.transactionScoped) {
      throw new Error("Nested transactions are not supported");
    }
    return this.connected().then((connectedAdaptor) =>
      connectedAdaptor.transaction(async (adaptor) => {
        // A statement that fails fails the transaction, even when the callback catches it and goes
        // on: PostgreSQL and CockroachDB refuse the rest of such a transaction, and the others
        // would commit around the failure.
        let failure: { error: unknown } | undefined;
        const options = { ...this.options, adaptor: trackFailures(adaptor, (error) => (failure ??= { error })) };
        try {
          let result: TResult;
          try {
            result = await callback(new Database(options, true, this.withClause));
          } catch (error) {
            // The first failed statement, rather than PostgreSQL's refusal of the next one.
            throw failure ? failure.error : error;
          }
          if (failure) throw failure.error;
          return result;
        } finally {
          // A transaction kept past its callback runs nothing more, rather than outside it.
          options.adaptor = ENDED_TRANSACTION;
        }
      }),
    );
  }

  select<TTable extends Table>(table: TTable): SelectQueryBuilder<TTable, ValueOfTable<TTable>, number>;
  select<TTable extends Table, P extends Selection>(
    table: TTable,
    projection: P,
  ): SelectQueryBuilder<TTable, { [K in keyof P]: SelectionValue<P[K]> }, number, never, P>;
  select(table: Table, projection?: Selection): SelectQueryBuilder<any, any, number, any, any> {
    if (
      projection !== undefined &&
      (projection === null || Array.isArray(projection) || typeof projection !== "object")
    ) {
      throw new Error("select() expects a projection object; omit it to select all columns");
    }
    const entries = projection && Object.entries(projection);
    // Keep ordinary column reads on the adaptor's existing fast path, including scalar inserts.
    const plain =
      entries?.length &&
      entries.every(([key, value]) => isFieldReference(value) && value.table === table && String(value.key) === key);
    const query: SelectQuery = {
      table,
      fields: plain ? entries!.map(([, value]) => value as SingleFieldBinding) : getFieldBindingsByKeys(table, ["*"]),
      projection: plain ? undefined : projection,
      where: undefined,
      orderBy: [],
      limit: undefined,
      offset: undefined,
      with: this.withClause,
    };
    return createSelectQueryBuilder(query, this.options.adaptor) as any;
  }

  /** Deletes the rows `.where(…)` matches; running it without a condition throws. */
  delete<TTable extends Table>(table: TTable): DeleteWrite<TTable> {
    return createDelete(() => this.options.adaptor, table, undefined, this.withClause);
  }

  /**
   * Inserts a row, or several. Await it, or chain `.returning(…)` for the rows it wrote and
   * `.onConflict(key).doNothing()` / `.doUpdate(…)` for rows that conflict with existing ones. With
   * only a table, `.select(query)` inserts what the query gives.
   */
  insert<TTable extends Table>(
    table: TTable,
  ): {
    /** `INSERT INTO table (columns) <query>`: the query's projection names the columns. */
    select(query: SelectQueryBuilder<any, any, any, any, any>): InsertSelectWrite<TTable>;
  };
  insert<TTable extends Table>(table: TTable, values: InputOfTable<TTable>): InsertWrite<TTable, true>;
  insert<TTable extends Table>(table: TTable, values: InputOfTable<TTable>[]): InsertWrite<TTable, false>;
  insert<TTable extends Table>(table: TTable, values: InsertValues<TTable>[]): SubqueryInsertWrite<TTable, false>;
  insert<TTable extends Table>(table: TTable, values: InsertValues<TTable>): SubqueryInsertWrite<TTable, true>;
  insert<TTable extends Table>(table: TTable, values?: InsertValues<TTable> | InsertValues<TTable>[]): unknown {
    const adaptor = () => this.options.adaptor;
    const clause = this.withClause;
    if (values === undefined)
      return { select: (query: any) => createInsertSelect(adaptor, table, query, undefined, clause) };
    return Array.isArray(values)
      ? createInsert(adaptor, table, values, false, undefined, clause)
      : createInsert(adaptor, table, [values], true, undefined, clause);
  }

  /** Updates each row `.by(key)` finds to that row's own values. */
  update<TTable extends Table>(table: TTable, rows: Partial<InputOfTable<TTable>>[]): UpdateRows<TTable>;
  /** Updates the rows `.where(…)` matches to `values`; running it without a condition throws. */
  update<TTable extends Table>(table: TTable, values: UpdateValues<TTable>): UpdateWrite<TTable>;
  update<TTable extends Table>(
    table: TTable,
    values: UpdateValues<TTable> | Partial<InputOfTable<TTable>>[],
  ): UpdateWrite<TTable> | UpdateRows<TTable> {
    const adaptor = () => this.options.adaptor;
    const clause = this.withClause;
    if (!Array.isArray(values)) return createUpdate(adaptor, table, values, undefined, clause);
    return {
      by: (key) => createUpdateRows(adaptor, table, values, Array.isArray(key) ? key : [key as any], undefined, clause),
    } as UpdateRows<TTable>;
  }

  /**
   * The SQL that creates these tables on an empty database, in order: each table, its indexes and
   * its settings, as `syncTable` would apply them. Running it once is much faster than syncing each
   * table, which inspects the database first, when the database is known to be empty.
   */
  schemaSql(tables: readonly Table[]): string[] {
    return tables
      .flatMap((table) => [
        this.adaptor.createTableStatement(table),
        ...table.indexes.flatMap((index) => this.adaptor.createIndexStatements(table, index)),
        ...this.adaptor.tableSettingStatements(table),
      ])
      .map((statement) => statement[TO_SQL_SYMBOL]().trim());
  }

  /** Syncs one table as `syncTables` does, returning what it changed, or undefined for nothing. */
  async syncTable<TTable extends Table>(table: TTable): Promise<TableSyncChanges | undefined> {
    return (await this.syncTables([table]))[0];
  }

  /**
   * Brings each table up to its definition: creates it, adds, removes and changes columns, its
   * primary key and foreign keys, creates and rebuilds indexes, and sets where its rows live. The
   * tables are read together first (`fetchLiveSchema`), in a few schema-wide queries, so a current
   * schema costs those reads and no writes; tables outside `tables` are left alone. Returns what it
   * changed, one entry per table it changed.
   */
  async syncTables(tables: readonly Table[]): Promise<TableSyncChanges[]> {
    await this.connected();
    const live = await this.adaptor.fetchLiveSchema(tables);
    const changes: TableSyncChanges[] = [];
    for (const table of tables) {
      // `null`: the database doesn't hold it; `undefined`: the adaptor reads tables one at a time.
      const current = live ? (live.get(String(table.id)) ?? null) : undefined;
      const change = await this.syncLiveTable(table, current);
      if (change.created || change.columns.length || change.primaryKey || change.indexes.length || change.locality)
        changes.push(change);
    }
    return changes;
  }

  private async syncLiveTable(table: Table, current: LiveTableSchema | null | undefined): Promise<TableSyncChanges> {
    const change: TableSyncChanges = {
      table: String(table.id),
      created: false,
      columns: [],
      primaryKey: false,
      indexes: [],
      locality: false,
    };
    const columns = current === undefined ? (await this.adaptor.fetchTableColumns(table)).results : current?.columns;
    if (!columns?.length) {
      await this.adaptor.createTable(table);
      change.created = true;
      change.indexes = await this.adaptor.syncTableIndexes(table, []);
      change.locality = await this.adaptor.syncTableLocality(table);
      return change;
    }
    const diff = this.diffTable(table, columns);
    if (diff.fields.length > 0 || diff.primaryKey) {
      try {
        await this.adaptor.processDiff(table, diff);
      } catch (error) {
        // Dropped since it was read: created as it is now defined.
        if ((await this.adaptor.fetchTableColumns(table)).results.length) throw error;
        return this.syncLiveTable(table, null);
      }
      change.columns = diff.fields.map((field) => String(field.key));
      change.primaryKey = Boolean(diff.primaryKey);
    }
    // Changing columns can drop the indexes on them, so those are read again.
    const changedColumns = change.columns.length > 0 || change.primaryKey;
    change.indexes = await this.adaptor.syncTableIndexes(table, changedColumns ? undefined : current?.indexes);
    change.locality = await this.adaptor.syncTableLocality(table, current ?? undefined);
    return change;
  }

  /** Every table in the database's current schema: its columns, indexes and unique constraints. */
  describeSchema(): Promise<SchemaTableInfo[]> {
    return this.adaptor.describeSchema();
  }

  /**
   * What the database holds that these table definitions don't: other tables with their row counts,
   * and other columns, indexes and unique constraints on the defined tables. `syncTable` only adds,
   * so this is how to find what a removal or rename left behind. Read-only: it changes nothing.
   */
  async diffSchema(tables: readonly Table[]): Promise<SchemaDrift> {
    await this.connected();
    const defined = new Map(tables.map((table) => [String(table.id), table]));
    const drift: SchemaDrift = { tables: [], columns: [], indexes: [] };
    const byName = (left: { name: string }, right: { name: string }) => (left.name < right.name ? -1 : 1);
    for (const live of [...(await this.describeSchema())].sort(byName)) {
      const table = defined.get(live.name);
      if (!table) {
        const count = this.adaptor.quoteIdentifier("count");
        const { first } = await this.adaptor.execute(
          raw(`SELECT COUNT(*) AS ${count} FROM ${this.adaptor.quoteIdentifier(live.name)}`),
        );
        drift.tables.push({ name: live.name, rowCount: Number(first?.count ?? 0) });
        continue;
      }
      for (const column of [...live.columns].sort()) {
        // biome-ignore lint/suspicious/noPrototypeBuiltins: The library targets ES2015.
        if (!Object.prototype.hasOwnProperty.call(table.fields, column))
          drift.columns.push({ table: live.name, column });
      }
      const indexes = new Set(table.indexes.map((index) => index.id));
      for (const index of [...live.indexes].sort(byName))
        if (!indexes.has(index.name)) drift.indexes.push({ table: live.name, index: index.name, unique: index.unique });
    }
    return drift;
  }

  private get adaptor() {
    return this.options.adaptor;
  }

  private diffTable<TTable extends Table>(table: TTable, remoteColumns: readonly TableColumnInfo[]): TableDiff {
    const diff: TableDiff = {
      fields: [],
    };

    if (remoteColumns.length === 0) {
      return diff;
    }

    for (const column of remoteColumns) {
      const field = Object.entries(table.fields).find(([key]) => key === column.name)?.[1];
      if (!field) {
        diff.fields.push({ key: column.name, type: "removed" });
      }
    }

    // A one-column key keeps its own field's change; any other key change is the table's.
    const expectedPrimaryKey = getPrimaryKey(table);
    const remotePrimaryKey = remoteColumns.filter((column) => column.primaryKey).map((column) => column.name);
    const samePrimaryKey =
      remotePrimaryKey.length === expectedPrimaryKey.length &&
      expectedPrimaryKey.every((key) => remotePrimaryKey.includes(key));
    if (expectedPrimaryKey.length > 1 && !samePrimaryKey) diff.primaryKey = expectedPrimaryKey;

    for (const [key, value] of Object.entries(table.fields)) {
      const field = value as SingleFieldBinding;
      const column = remoteColumns.find((column) => column.name === key);
      if (!column) {
        diff.fields.push({
          key: field.key,
          field,
          type: "added",
        });
      } else {
        // A column can't become computed or stop being so in place: that is a migration's.
        const isComputed = Boolean(getMetaItem(field.schema, computed));
        if (column.generated !== undefined && column.generated !== isComputed) {
          throw new Error(
            `[zodbase] Column '${key}' of '${String(table.id)}' is ${column.generated ? "" : "not "}computed in the database but ${isComputed ? "is" : "isn't"} in its definition: migrate it first`,
          );
        }
        // Its expression decides its value, and with it whether it can be null.
        if (isComputed) continue;
        const isRequired = isZodRequired(field.schema);
        const modifications: FieldModification[] = [];
        // Only widen legacy 32-bit floats; never infer destructive type conversions. Integer fields
        // widen too: columns created before zod 4 integer checks were recognised are REAL, which
        // sends millisecond timestamps rounded to the nearest 32-bit float.
        if (
          ["REAL", "FLOAT4"].includes(column.sqlType?.toUpperCase() ?? "") &&
          isZodTypeExtends(field.schema, zod.ZodNumber) &&
          this.adaptor.typeToSql(zod.number()) === "DOUBLE PRECISION"
        ) {
          modifications.push({ type: "widen-number" });
        }
        if (
          /^datetime(?:\([012]\))?$/i.test(column.sqlType ?? "") &&
          this.adaptor.typeToSql(field.schema) === "DATETIME(3)"
        ) {
          modifications.push({ type: "widen-date" });
        }
        if (column.notNull !== isRequired) {
          modifications.push({
            type: isRequired ? "add-constraint" : "remove-constraint",
            constraint: "NOT NULL",
          });
        }
        const foreignKeyMeta = getMetaItem(field.schema, foreignKey);
        if (expectedPrimaryKey.length === 1 && expectedPrimaryKey[0] === key && !column.primaryKey) {
          if (remoteColumns.some((existingColumn) => existingColumn.primaryKey)) {
            throw new Error(
              `[zodbase] Cannot add primary key '${key}' to '${String(table.id)}': another primary key already exists`,
            );
          }
          modifications.push({ type: "add-constraint", constraint: "PRIMARY KEY" });
        }
        const expectedForeignKey = foreignKeyMeta
          ? {
              table: String(foreignKeyMeta.data.field.table.id),
              field: String(foreignKeyMeta.data.field.key),
              onDelete: foreignKeyMeta.data.onDelete ?? ("no action" as const),
            }
          : undefined;
        const actualForeignKey = column.foreignKey;
        const foreignKeyMatches =
          actualForeignKey?.table === expectedForeignKey?.table &&
          actualForeignKey?.field === expectedForeignKey?.field &&
          actualForeignKey?.onDelete === expectedForeignKey?.onDelete;
        if (!foreignKeyMatches) {
          if (actualForeignKey) {
            modifications.push({ type: "remove-foreign-key", foreignKey: actualForeignKey });
          }
          if (expectedForeignKey) {
            modifications.push({ type: "add-foreign-key", foreignKey: expectedForeignKey });
          }
        }
        if (modifications.length > 0) {
          diff.fields.push({ key: field.key, field, type: "modified", modifications });
        }
      }
    }

    return diff;
  }
}

export const mapSqlResult = <TFrom, TTo, TResultLimit extends number>(
  result: SqlResult<TFrom, TResultLimit>,
  mapper: (from: TFrom) => TTo | undefined,
): SqlResult<TTo, TResultLimit> => {
  const mapped = (result.results as any).reduce((acc: TTo[], result: any) => {
    const mapped = mapper(result);
    if (mapped !== undefined) {
      acc.push(mapped);
    }
    return acc;
  }, []);

  return {
    ...result,
    first: mapped.length > 0 ? mapped[0] : undefined,
    results: mapped,
  } as SqlResult<TTo, TResultLimit>;
};

export { metaStore } from "zod-meta";
export { exists, notExists } from "./Bindings";
export type { default as DatabaseAdaptor } from "./DatabaseAdaptor";
export * from "./MetaTypes";
export type { SelectCondition, SelectQueryBuilder } from "./QueryBuilder";
export { type Compilable, union, unionAll } from "./QueryStatement";
export {
  add,
  coalesce,
  divide,
  expression,
  greatest,
  IncomingValue,
  incoming,
  scalar,
  subtract,
  sum,
} from "./SqlExpression";
export type {
  ConflictActions,
  ConflictOptions,
  DeleteWrite,
  InsertSelectWrite,
  InsertWrite,
  UpdateRows,
  UpdateRowsWrite,
  UpdateWrite,
  Write,
  WriteResult,
} from "./Writes";
