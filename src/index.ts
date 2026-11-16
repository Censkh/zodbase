import { assertSelectArguments, type InsertValues, isFieldReference, SELECT_QUERY } from "./QueryBuilder";
import { COMPILE, type Compilable, isCompilable, QueryStatement, toQueryStatement } from "./QueryStatement";
import { isRelationalQuery } from "./RelationalQuery";
import { snapshotQuery } from "./Subquery";
import {
  createDelete,
  createInsert,
  createInsertSelect,
  createUpdate,
  createUpdateEach,
  type DeleteWrite,
  type InsertSelectWrite,
  type InsertWrite,
  isWrite,
  runBatch,
  type SubqueryInsertWrite,
  type UpdateEachWrite,
  type UpdateWrite,
  type Write,
  type WriteResult,
} from "./Writes";

export type { InsertValues, ScalarSubquery } from "./QueryBuilder";

import * as zod from "zod";
import { getMetaItem } from "zod-meta";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import { escapeSqlValue } from "./Escaping";
import { toLazyPromise } from "./LazyPromise";
import { foreignKey } from "./MetaTypes";
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
  UpdateValues,
  ValueOfTable,
} from "./QueryBuilder";
import { type Statement, TO_SQL_SYMBOL, type ToSql } from "./Statement";
import { getPrimaryKey, type SchemaDrift, type SchemaTableInfo, type Table, type TableIndex } from "./Table";
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

export type DatabaseAdaptorInitializer = () => DatabaseAdaptor | Promise<DatabaseAdaptor>;

export interface DatabaseOptions {
  adaptor: DatabaseAdaptor | DatabaseAdaptorInitializer;
  events?: DatabaseEvents;
  debug?: boolean;
}

interface ResolvedDatabaseOptions extends Omit<DatabaseOptions, "adaptor"> {
  adaptor: DatabaseAdaptor;
}

/** On a lazily initialized adaptor, resolves to the adaptor it initializes. */
const RESOLVE_ADAPTOR = Symbol("zodbase.resolveAdaptor");

const createLazyDatabaseAdaptor = (initialize: DatabaseAdaptorInitializer): DatabaseAdaptor => {
  let adaptorPromise: Promise<DatabaseAdaptor> | undefined;
  let resolved: DatabaseAdaptor | undefined;
  const getAdaptor = () =>
    (adaptorPromise ??= Promise.resolve()
      .then(initialize)
      .then((adaptor) => (resolved = adaptor)));
  const call =
    (method: keyof DatabaseAdaptor) =>
    async (...args: unknown[]) => {
      const adaptor = await getAdaptor();
      return (adaptor[method] as (...args: unknown[]) => unknown).apply(adaptor, args);
    };

  const initialized = () => {
    if (!resolved) throw new Error("Database adaptor has not initialized");
    return resolved;
  };

  return {
    typeToSql: (type: zod.ZodType) => initialized().typeToSql(type),
    quoteIdentifier: (value: string) => initialized().quoteIdentifier(value),
    createTableStatement: (table: Table, name?: string) => initialized().createTableStatement(table, name),
    createIndexStatement: (table: Table, index: TableIndex) => initialized().createIndexStatement(table, index),
    tableSettingStatements: (table: Table) => initialized().tableSettingStatements(table),
    execute: call("execute"),
    compileStatement: call("compileStatement"),
    executeSelect: call("executeSelect"),
    executeQueryCount: call("executeQueryCount"),
    executeCappedCount: call("executeCappedCount"),
    executeInsert: call("executeInsert"),
    executeInsertSubqueries: call("executeInsertSubqueries"),
    executeInsertMany: call("executeInsertMany"),
    executeUpdate: call("executeUpdate"),
    executeUpsert: call("executeUpsert"),
    executeUpsertMany: call("executeUpsertMany"),
    executeUpdateMany: call("executeUpdateMany"),
    executeCount: call("executeCount"),
    executeDelete: call("executeDelete"),
    executeInsertIgnoring: call("executeInsertIgnoring"),
    executeTogether: call("executeTogether"),
    [RESOLVE_ADAPTOR]: getAdaptor,
    createTable: call("createTable"),
    fetchTableColumns: call("fetchTableColumns"),
    syncTableIndexes: call("syncTableIndexes"),
    syncTableLocality: call("syncTableLocality"),
    processDiff: call("processDiff"),
    transaction: call("transaction"),
  } as unknown as DatabaseAdaptor;
};

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
      return adaptor.executeSelect(query);
    },
    /**
     * The number of rows the query matches, ignoring its ordering, limit and offset. With `upTo`, it
     * reads at most that many, so counting a large match stays cheap: a result of `upTo` means "at
     * least `upTo`".
     */
    async count(options: { upTo?: number } = {}): Promise<number> {
      if (options.upTo !== undefined) return adaptor.executeCappedCount(query, options.upTo);
      const all = [{ table: query.table, key: "*" }] as any;
      const result = isRelationalQuery(query)
        ? await adaptor.executeQueryCount({ ...query, fields: all })
        : await adaptor.executeCount(query.table, all, query.where);
      return Number((result.first as { _count?: unknown } | undefined)?._count ?? 0);
    },
  };

  return toLazyPromise(() => {
    assertSelectArguments(query);
    return adaptor.executeSelect(query);
  }, builder) as any;
};

export class Database {
  private readonly options: ResolvedDatabaseOptions;

  constructor(
    options: DatabaseOptions,
    private readonly transactionScoped = false,
  ) {
    this.options = {
      ...options,
      adaptor: typeof options.adaptor === "function" ? createLazyDatabaseAdaptor(options.adaptor) : options.adaptor,
    };
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
    const adaptor = this.options.adaptor;
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
    return (await runBatch(await this.resolvedAdaptor(), writes)) as any;
  }

  /** The adaptor, initialized: a lazy one (`DatabaseOptions.adaptor` as a function) first resolves. */
  private async resolvedAdaptor(): Promise<DatabaseAdaptor> {
    const adaptor = this.options.adaptor as DatabaseAdaptor & { [RESOLVE_ADAPTOR]?: () => Promise<DatabaseAdaptor> };
    return adaptor[RESOLVE_ADAPTOR] ? adaptor[RESOLVE_ADAPTOR]() : adaptor;
  }

  transaction<TResult>(callback: (transaction: Database) => Promise<TResult>): Promise<TResult> {
    if (this.transactionScoped) {
      throw new Error("Nested transactions are not supported");
    }
    return this.options.adaptor.transaction((adaptor) =>
      callback(
        new Database(
          {
            ...this.options,
            adaptor,
          },
          true,
        ),
      ),
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
    };
    return createSelectQueryBuilder(query, this.options.adaptor) as any;
  }

  /** Deletes the rows `.where(…)` matches; running it without a condition throws. */
  delete<TTable extends Table>(table: TTable): DeleteWrite<TTable> {
    return createDelete(() => this.options.adaptor, table);
  }

  /**
   * Inserts a row, or several. Await it, or chain `.returning(…)` for the rows it wrote and
   * `.onConflict(key).doNothing()` / `.doUpdate()` for rows that conflict with existing ones. With
   * only a table, `.fromSelect(select)` inserts what the select gives.
   */
  insert<TTable extends Table>(
    table: TTable,
  ): {
    /** `INSERT INTO table (columns) <select>`: the select's projection names the columns. */
    fromSelect(select: SelectQueryBuilder<any, any, any, any, any>): InsertSelectWrite<TTable>;
  };
  insert<TTable extends Table>(table: TTable, values: InputOfTable<TTable>): InsertWrite<TTable, true>;
  insert<TTable extends Table>(table: TTable, values: InputOfTable<TTable>[]): InsertWrite<TTable, false>;
  insert<TTable extends Table>(table: TTable, values: InsertValues<TTable>[]): SubqueryInsertWrite<TTable, false>;
  insert<TTable extends Table>(table: TTable, values: InsertValues<TTable>): SubqueryInsertWrite<TTable, true>;
  insert<TTable extends Table>(table: TTable, values?: InsertValues<TTable> | InsertValues<TTable>[]): unknown {
    const adaptor = () => this.options.adaptor;
    if (values === undefined) return { fromSelect: (select: any) => createInsertSelect(adaptor, table, select) };
    return Array.isArray(values)
      ? createInsert(adaptor, table, values, false)
      : createInsert(adaptor, table, [values], true);
  }

  /** Updates the rows `.where(…)` matches to `values`; running it without a condition throws. */
  update<TTable extends Table>(table: TTable, values: UpdateValues<TTable>): UpdateWrite<TTable> {
    return createUpdate(() => this.options.adaptor, table, values);
  }

  /** Updates each row whose `by` field matches one of `rows` to that row's other values. */
  updateEach<TTable extends Table>(
    table: TTable,
    rows: Partial<InputOfTable<TTable>>[],
    { by }: { by: SingleFieldBinding<ValueOfTable<TTable>> },
  ): UpdateEachWrite<TTable> {
    return createUpdateEach(() => this.options.adaptor, table, rows, by);
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
        ...table.indexes.map((index) => this.adaptor.createIndexStatement(table, index)),
        ...this.adaptor.tableSettingStatements(table),
      ])
      .map((statement) => statement[TO_SQL_SYMBOL]().trim());
  }

  async syncTable<TTable extends Table>(table: TTable): Promise<void> {
    await this.options.adaptor.createTable(table);

    const diff = await this.diffTable(table);

    if (diff.fields.length > 0 || diff.primaryKey) {
      await this.adaptor.processDiff(table, diff);
    }

    if (table.indexes.length > 0) {
      await this.adaptor.syncTableIndexes(table);
    }
    await this.adaptor.syncTableLocality(table);
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

  private async diffTable<TTable extends Table>(table: TTable): Promise<TableDiff> {
    const tableRemoteSchema = await this.options.adaptor.fetchTableColumns(table);

    const diff: TableDiff = {
      fields: [],
    };

    if (tableRemoteSchema.results.length === 0) {
      return diff;
    }

    for (const column of tableRemoteSchema.results) {
      const field = Object.entries(table.fields).find(([key]) => key === column.name)?.[1];
      if (!field) {
        diff.fields.push({ key: column.name, type: "removed" });
      }
    }

    // A one-column key keeps its own field's change; any other key change is the table's.
    const expectedPrimaryKey = getPrimaryKey(table);
    const remotePrimaryKey = tableRemoteSchema.results
      .filter((column) => column.primaryKey)
      .map((column) => column.name);
    const samePrimaryKey =
      remotePrimaryKey.length === expectedPrimaryKey.length &&
      expectedPrimaryKey.every((key) => remotePrimaryKey.includes(key));
    if (expectedPrimaryKey.length > 1 && !samePrimaryKey) diff.primaryKey = expectedPrimaryKey;

    for (const [key, value] of Object.entries(table.fields)) {
      const field = value as SingleFieldBinding;
      const column = tableRemoteSchema.results.find((column) => column.name === key);
      if (!column) {
        diff.fields.push({
          key: field.key,
          field,
          type: "added",
        });
      } else {
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
          if (tableRemoteSchema.results.some((existingColumn) => existingColumn.primaryKey)) {
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
export { exists } from "./Bindings";
export type { default as DatabaseAdaptor } from "./DatabaseAdaptor";
export * from "./MetaTypes";
export type { SelectCondition, SelectQueryBuilder } from "./QueryBuilder";
export { type Compilable, union, withCtes } from "./QueryStatement";
export { add, coalesce, divide, expression, scalar, subtract, sum } from "./SqlExpression";
export type {
  DeleteWrite,
  InsertSelectWrite,
  InsertWrite,
  UpdateEachWrite,
  UpdateWrite,
  Write,
  WriteResult,
} from "./Writes";
