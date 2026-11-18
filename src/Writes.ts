import type * as zod from "zod";
import { findFieldMetaItems } from "zod-meta";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import { type Conflict, incomingFields, keyCondition } from "./DatabaseAdaptor";
import { toLazyPromise } from "./LazyPromise";
import { monotonicTimestamp, updatedAt } from "./MetaTypes";
import type {
  BindingKeys,
  InputOfTable,
  InsertValues,
  SelectCondition,
  SingleFieldBinding,
  SqlDefiniteResult,
  SqlResult,
  UpdateValues,
  ValueOfTable,
} from "./QueryBuilder";
import { isFieldReference, isScalarSelect, SELECT_QUERY } from "./QueryBuilder";
import { COMPILE, type Compilable, toQueryStatement, type WithClause } from "./QueryStatement";
import { SqlExpression } from "./SqlExpression";
import type { Statement } from "./Statement";
import { hasSubqueries, isSubquery, snapshotCondition, snapshotQuery } from "./Subquery";
import { computedFields, type Table, withComputed, withoutComputed } from "./Table";

/**
 * A write as `Database.batch` and writable CTEs take it: how to run it on its own, and the one
 * statement it is where the adaptor can build it (`buildInsertStatement` and the like), with how to
 * read that statement's result.
 */
export interface WritePlan {
  /** Whether it writes nothing, and runs no statement. */
  readonly empty?: boolean;
  run(adaptor: DatabaseAdaptor): Promise<SqlResult>;
  build(adaptor: DatabaseAdaptor): { statement: Statement; read(result: SqlResult): SqlResult } | undefined;
}

export const WRITE = Symbol("zodbase.write");

/** An insert, update or delete: awaiting it runs it; `Database.batch` runs several together. */
export interface Write<TResult extends SqlResult = SqlResult<void, 0>> extends Promise<TResult> {
  readonly [WRITE]: WritePlan;
  [COMPILE](adaptor: DatabaseAdaptor): Statement;
}

const EMPTY: SqlResult<void, 0> = { results: [], first: undefined };

const noRows = <TResult extends SqlResult>(result: SqlResult): TResult =>
  (result.results.length ? result : { results: [], first: undefined }) as TResult;

/** Keeps only `keys` of each row, when given (all columns for none or `*`). */
const pickKeys = <TResult extends SqlResult>(result: TResult, keys: readonly string[]): TResult => {
  if (!keys.length || keys.includes("*")) return result;
  const results = result.results.map((row: Record<string, unknown>) =>
    Object.fromEntries(keys.map((key) => [key, row[key]])),
  );
  return { ...result, results, first: results[0] } as TResult;
};

/**
 * `plan` starting with `clause`: it runs as the one statement it builds, which only PostgreSQL and
 * CockroachDB build for a write.
 */
const withClausePlan = (plan: WritePlan, clause: WithClause | undefined): WritePlan => {
  if (!clause) return plan;
  const build = (target: DatabaseAdaptor) => {
    const built = plan.build(target);
    return built && { statement: target.prefixWith(clause, built.statement), read: built.read };
  };
  return {
    empty: plan.empty,
    build,
    async run(target) {
      if (plan.empty) return plan.run(target);
      const built = build(target);
      if (!built) throw new Error("[zodbase] This database can't run this write after a WITH clause");
      return built.read(await target.execute(built.statement));
    },
  };
};

/** Wraps a plan as an awaitable `Write` with `extra` members, run after `clause` when given. */
const write = <TResult extends SqlResult, TExtra extends object>(
  adaptor: () => DatabaseAdaptor,
  unscoped: WritePlan,
  extra: TExtra,
  clause?: WithClause,
): Write<TResult> & TExtra => {
  const plan = withClausePlan(unscoped, clause);
  return toLazyPromise(() => plan.run(adaptor()) as Promise<TResult>, {
    ...extra,
    [WRITE]: plan,
    [COMPILE]: (target: DatabaseAdaptor) => {
      const built = plan.build(target);
      if (!built) throw new Error("[zodbase] This database can't compile this write as a statement");
      return built.statement;
    },
  }) as unknown as Write<TResult> & TExtra;
};

/** The values an update sets, parsed, with its `updatedAt` and monotonic timestamp columns stamped. */
export const parseUpdateValues = (table: Table, values: Record<string, unknown>) => {
  const schema = table.schema as zod.ZodObject<any>;
  const parsed: Record<string, unknown> = {};
  // The database computes these; an update can't set them.
  const computedKeys = new Set(computedFields(table).map((field) => field.key));
  for (const key of Object.keys(values)) {
    if (computedKeys.has(key)) continue;
    // biome-ignore lint/suspicious/noPrototypeBuiltins: Consumers may target Object APIs older than ES2022.
    if (!Object.prototype.hasOwnProperty.call(schema.shape, key)) continue;
    const value = values[key];
    // An expression or a column (`{ at: T.$at }`) is the database's to evaluate.
    parsed[key] = value instanceof SqlExpression || isFieldReference(value) ? value : schema.shape[key].parse(value);
  }
  // Stamped unless the update sets it itself, such as to its own column to leave it as it is.
  for (const field of findFieldMetaItems(table.schema, updatedAt))
    if (parsed[field.key] === undefined) parsed[field.key] = Date.now();
  for (const field of findFieldMetaItems(table.schema, monotonicTimestamp)) {
    const now = Date.now();
    parsed[field.key] = new SqlExpression((target) => {
      const column = target ? target.quoteIdentifier(String(field.key)) : `"${String(field.key)}"`;
      return `CASE WHEN ${column} >= ${now} THEN ${column} + 1 ELSE ${now} END`;
    }, table.fields[field.key]!.schema);
  }
  return parsed;
};

const conditionAnd = (current: SelectCondition | undefined, condition: SelectCondition) =>
  current ? current.and(condition) : condition;

const requireWhere = (kind: string, where: SelectCondition | undefined): SelectCondition => {
  if (!where) throw new Error(`[zodbase] ${kind} without where condition is not allowed`);
  return where;
};

const keyFields = (key: SingleFieldBinding | readonly SingleFieldBinding[]): readonly SingleFieldBinding[] => {
  const fields = Array.isArray(key) ? (key as readonly SingleFieldBinding[]) : [key as SingleFieldBinding];
  if (!fields.length) throw new Error("[zodbase] A conflict target needs at least one field");
  return fields;
};

/** What `onConflict(key, options)` builds on: the conflict target. */
export interface ConflictOptions<TTable extends Table> {
  /**
   * The condition of the partial unique index `key` is, which names that index as the target
   * (PostgreSQL, CockroachDB and SQLite).
   */
  where?: SelectCondition<ValueOfTable<TTable>>;
}

/** What to do with a row that conflicts with an existing one on the key `onConflict` names. */
export interface ConflictActions<TWrite, TTable extends Table> {
  /** Skips the row, keeping the existing one. */
  doNothing(): TWrite;
  /**
   * Updates the existing row: to `values` when given, where `incoming(field)` is the value the
   * inserted row brings (`{ count: add(T.$count, incoming(T.$count)) }` adds it), else to every
   * column the row gives but the key's. Rows with the same key in one insert keep the last.
   */
  doUpdate(values?: UpdateValues<TTable>): TWrite;
}

const conflictActions = <TWrite, TTable extends Table>(
  table: TTable,
  key: SingleFieldBinding | readonly SingleFieldBinding[],
  options: ConflictOptions<TTable> | undefined,
  build: (conflict: Conflict) => TWrite,
): ConflictActions<TWrite, TTable> => {
  const fields = keyFields(key);
  const where = options?.where && snapshotCondition(options.where);
  return {
    doNothing: () => build({ fields, where, action: "nothing" }),
    doUpdate: (values) => {
      if (values && !Object.keys(values).length) throw new Error("[zodbase] A conflict's update needs values to set");
      return build({
        fields,
        where,
        action: "update",
        set: values && parseUpdateValues(table, values as Record<string, unknown>),
      });
    },
  };
};

/** The rows of the `returning()` of an insert of one row, or of several. */
type InsertReturning<TTable extends Table, TOne extends boolean> = SqlDefiniteResult<
  ValueOfTable<TTable>,
  TOne extends true ? 1 : number
>;

export interface InsertWrite<TTable extends Table, TOne extends boolean> extends Write {
  /** The rows the insert writes, as its table's schema parsed them: no round trip needed. */
  readonly rows: ValueOfTable<TTable>[];
  /** Runs the insert, returning the rows it wrote (just `keys` of each, when given). */
  returning(...keys: BindingKeys<ValueOfTable<TTable>>[]): Write<InsertReturning<TTable, TOne>>;
  /**
   * What to do with a row that conflicts with an existing one on `key` (a unique field, or the
   * fields of a unique key in order): skip it (`doNothing`), or update the existing row
   * (`doUpdate`). With `doNothing`, `returning` gives only the rows inserted.
   */
  onConflict(
    key: SingleFieldBinding<ValueOfTable<TTable>> | readonly SingleFieldBinding<ValueOfTable<TTable>>[],
    options?: ConflictOptions<TTable>,
  ): ConflictActions<InsertWrite<TTable, TOne>, TTable>;
  /**
   * Inserts the rows only where `condition` holds: `INSERT … SELECT <values> WHERE condition`, the
   * values cast to their columns' types. The condition stands on its own, such as
   * `exists(db.select(Parent, …).where(…))`; no table is in scope for it. Several calls must all hold.
   */
  where(condition: SelectCondition): this;
}

/** An insert of values some of which are subqueries, run by the database: no parsed rows. */
export type SubqueryInsertWrite<TTable extends Table, TOne extends boolean> = Omit<InsertWrite<TTable, TOne>, "rows">;

export const createInsert = <TTable extends Table, TOne extends boolean>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  input: InsertValues<TTable>[],
  one: TOne,
  conflict?: Conflict,
  clause?: WithClause,
  condition?: SelectCondition,
): InsertWrite<TTable, TOne> => {
  const subqueries = hasSubqueries(input as object[]);
  const schema = table.schema as zod.ZodObject<any>;
  const rows = input.map((row) => {
    // Computed before parsing, so a row needn't give a computed column, and one that can't be
    // computed from it fails the schema.
    if (!subqueries)
      return schema.parse(withComputed(table, row as Record<string, unknown>)) as Record<string, unknown>;
    const expressions: Record<string, unknown> = {};
    const literals: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row as object)) {
      if (!isSubquery(value)) {
        literals[key] = value;
        continue;
      }
      // biome-ignore lint/suspicious/noPrototypeBuiltins: Support consumers targeting pre-ES2022 runtimes.
      if (!Object.prototype.hasOwnProperty.call(schema.shape, key)) throw new Error(`Unknown insert column: ${key}`);
      const query = value[SELECT_QUERY].query;
      if (!isScalarSelect(query)) throw new Error("Scalar subqueries must select exactly one column");
      // Snapshot the AST: later changes to a reusable builder must not change this insert.
      expressions[key] = { [SELECT_QUERY]: { query: snapshotQuery(query) } };
    }
    const mask = Object.fromEntries(Object.keys(expressions).map((key) => [key, true as const]));
    return { ...schema.omit(mask).parse(literals), ...expressions };
  });
  // What the database is sent: computed columns are its own.
  const stored = rows.map((row) => withoutComputed(table, row));

  const plan = (keys: readonly string[] | undefined): WritePlan => {
    const returning = keys !== undefined;
    // Adaptors that return the rows they were given don't have the computed columns.
    const complete = (result: SqlResult): SqlResult => {
      if (!computedFields(table).length || !result.results.length) return result;
      const results = result.results.map((row: Record<string, unknown>) => withComputed(table, row, true));
      return { ...result, results, first: results[0] };
    };
    const read = (result: SqlResult) => pickKeys(complete(result), keys ?? []);
    // Selected values: only where the condition holds, or subquery values that may conflict.
    const selected = condition !== undefined || (subqueries && conflict !== undefined);
    const buildSelected = (target: DatabaseAdaptor) => {
      const statement = target.buildConditionalInsertSql(
        table,
        stored,
        condition && snapshotCondition(condition),
        returning,
        conflict,
      );
      return {
        statement,
        read: (result: SqlResult) => (returning ? read(target.decode(table, result)) : noRows(result)),
      };
    };
    return {
      empty: !rows.length,
      async run(target) {
        if (!rows.length) return EMPTY;
        if (selected) {
          const built = buildSelected(target);
          return built.read(await target.execute(built.statement));
        }
        if (subqueries) return read(await target.executeInsertSubqueries(table, stored as any, returning));
        if (conflict?.action === "nothing")
          return read(await target.executeInsertIgnoring(table, stored as any, conflict, returning));
        if (conflict?.action === "update") {
          if (rows.length === 1) {
            const result = await target.executeUpsert(table, stored[0] as any, conflict);
            if (!returning) return EMPTY;
            if (result.results.length) return read(result);
            // Adaptors that can't return an upsert's row read it back by its key.
            const row = rows[0]!;
            const where = conflict.fields
              .map((field) => field.equals(row[String(field.key)] as any) as SelectCondition)
              .reduce((all, next) => all.and(next));
            return read(
              await target.executeSelect({
                table,
                fields: [{ table, key: "*" } as any],
                where,
                orderBy: [],
                limit: 1,
                offset: undefined,
              }),
            );
          }
          await target.executeUpsertMany(table, stored as any, conflict);
          if (!returning) return EMPTY;
          if (conflict.fields.length !== 1)
            throw new Error("[zodbase] Returning the rows of a multi-row upsert needs a single-field key");
          const [field] = conflict.fields;
          return read(
            await target.executeSelect({
              table,
              fields: [{ table, key: "*" } as any],
              where: field!.in(rows.map((row) => row[String(field!.key)]) as any) as SelectCondition,
              orderBy: [],
              limit: undefined,
              offset: undefined,
            }),
          );
        }
        const result =
          one && rows.length === 1
            ? await target.executeInsert(table, stored[0] as any, returning)
            : await target.executeInsertMany(table, stored as any, returning);
        return returning ? read(result) : EMPTY;
      },
      build(target) {
        if (!rows.length) return undefined;
        if (selected) return buildSelected(target);
        if (subqueries) return undefined;
        const statement = target.buildInsertStatement(table, stored, { returning, conflict });
        if (!statement) return undefined;
        return { statement, read: (result) => (returning ? read(target.decode(table, result)) : noRows(result)) };
      },
    };
  };

  return write(
    adaptor,
    plan(undefined),
    {
      rows: rows as ValueOfTable<TTable>[],
      returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}, clause),
      onConflict: (key: SingleFieldBinding | readonly SingleFieldBinding[], options?: ConflictOptions<TTable>) =>
        conflictActions(table, key, options, (next) =>
          createInsert(adaptor, table, input, one, next, clause, condition),
        ),
      where: (next: SelectCondition) =>
        createInsert(adaptor, table, input, one, conflict, clause, conditionAnd(condition, next)),
    },
    clause,
  ) as unknown as InsertWrite<TTable, TOne>;
};

export interface InsertSelectWrite<TTable extends Table> extends Write {
  /** Runs the insert, returning the rows it wrote (just `keys` of each, when given). */
  returning(...keys: BindingKeys<ValueOfTable<TTable>>[]): Write<SqlResult<ValueOfTable<TTable>>>;
  /**
   * What to do with a selected row that conflicts with an existing one on `key`, as an insert of
   * values does (`InsertWrite.onConflict`). Not on SQL Server.
   */
  onConflict(
    key: SingleFieldBinding<ValueOfTable<TTable>> | readonly SingleFieldBinding<ValueOfTable<TTable>>[],
    options?: ConflictOptions<TTable>,
  ): ConflictActions<InsertSelectWrite<TTable>, TTable>;
}

/** `INSERT INTO table (columns) <select>`: the select's projection names the columns. */
export const createInsertSelect = <TTable extends Table>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  select: Compilable & { [SELECT_QUERY]: { query: { projection?: object } } },
  conflict?: Conflict,
  clause?: WithClause,
): InsertSelectWrite<TTable> => {
  const projection = select[SELECT_QUERY].query.projection;
  if (!projection) throw new Error("[zodbase] An insert from a select needs the select's projection");
  const columns = Object.keys(projection);
  // biome-ignore lint/suspicious/noPrototypeBuiltins: The library targets ES2015.
  if (!columns.length || columns.some((key) => !Object.prototype.hasOwnProperty.call(table.fields, key)))
    throw new Error("[zodbase] Invalid insert projection");
  const source = toQueryStatement(select);
  const plan = (keys: readonly string[] | undefined): WritePlan => {
    const returning = keys !== undefined;
    const build = (target: DatabaseAdaptor) => {
      const statement = target.buildInsertSelectSql(table, columns, source.compile(target), returning, conflict);
      return {
        statement,
        read: (result: SqlResult) => (returning ? pickKeys(target.decode(table, result), keys) : noRows(result)),
      };
    };
    return {
      async run(target) {
        const built = build(target);
        return built.read(await target.execute(built.statement));
      },
      build,
    };
  };
  return write(
    adaptor,
    plan(undefined),
    {
      returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}, clause),
      onConflict: (key: SingleFieldBinding | readonly SingleFieldBinding[], options?: ConflictOptions<TTable>) =>
        conflictActions(table, key, options, (next) => createInsertSelect(adaptor, table, select, next, clause)),
    },
    clause,
  ) as unknown as InsertSelectWrite<TTable>;
};

export interface UpdateWrite<TTable extends Table> extends Write {
  /** Narrows the rows it updates; several calls must all hold. */
  where(condition: SelectCondition<ValueOfTable<TTable>>): UpdateWrite<TTable>;
  /** Runs the update, returning the rows it changed (just `keys` of each, when given). */
  returning(...keys: BindingKeys<ValueOfTable<TTable>>[]): Write<SqlDefiniteResult<ValueOfTable<TTable>, number>>;
}

export const createUpdate = <TTable extends Table>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  values: UpdateValues<TTable>,
  where?: SelectCondition,
  clause?: WithClause,
): UpdateWrite<TTable> => {
  if (Object.keys(values).length === 0) throw new Error("[zodbase] No values to update");
  const parsed = parseUpdateValues(table, values as Record<string, unknown>);
  // Checked when it runs: an invalid condition rejects the update, before any SQL.
  const condition = where;
  const plan = (keys: readonly string[] | undefined): WritePlan => {
    const returning = keys !== undefined;
    const read = (result: SqlResult) => pickKeys(result, keys ?? []);
    return {
      async run(target) {
        const filter = snapshotCondition(requireWhere("Update", condition));
        const result = await target.executeUpdate(table, parsed as any, filter as any, returning);
        if (!returning) return EMPTY;
        if (result.selected) return read(result);
        // Adaptors that can't return updated rows read them back.
        return read(
          await target.executeSelect({
            table,
            fields: [{ table, key: "*" } as any],
            where: filter,
            orderBy: [],
            limit: undefined,
            offset: undefined,
          }),
        );
      },
      build(target) {
        const filter = snapshotCondition(requireWhere("Update", condition));
        const statement = target.buildUpdateStatement(table, parsed, filter, returning);
        if (!statement) return undefined;
        return { statement, read: (result) => (returning ? read(target.decode(table, result)) : noRows(result)) };
      },
    };
  };
  return write(
    adaptor,
    plan(undefined),
    {
      where: (next: SelectCondition) => createUpdate(adaptor, table, values, conditionAnd(condition, next), clause),
      returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}, clause),
    },
    clause,
  ) as unknown as UpdateWrite<TTable>;
};

/** `update(table, rows)`: each row's own values, waiting for the key that finds its row. */
export interface UpdateRows<TTable extends Table> {
  /**
   * Updates each row whose `key` (a field, or the fields of a key in order) matches one of the
   * rows to that row's other values; the fields a row leaves out stay as they are. A row matching
   * no row changes nothing, and a key given more than once keeps its last row.
   */
  by(
    key: SingleFieldBinding<ValueOfTable<TTable>> | readonly SingleFieldBinding<ValueOfTable<TTable>>[],
  ): UpdateRowsWrite<TTable>;
}

export interface UpdateRowsWrite<TTable extends Table> extends Write {
  /**
   * Updates a row only where `condition` also holds; several calls must all hold. `incoming(field)`
   * in it is the value the row gives that field: `.where(T.$at.lessThan(incoming(T.$at)))` moves
   * each row's `at` forward only. An update with a condition can't return its rows, since those it
   * skipped can't be told from those it changed afterwards.
   */
  where(condition: SelectCondition<ValueOfTable<TTable>>): Omit<UpdateRowsWrite<TTable>, "returning">;
  /** Runs the update, returning the rows it changed (just `keys` of each, when given). */
  returning(...keys: BindingKeys<ValueOfTable<TTable>>[]): Write<SqlDefiniteResult<ValueOfTable<TTable>, number>>;
}

/** Updates each of `rows` found by its `by` fields to its other values. */
export const createUpdateRows = <TTable extends Table>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  rows: Partial<InputOfTable<TTable>>[],
  by: readonly SingleFieldBinding<ValueOfTable<TTable>>[],
  where?: SelectCondition,
  clause?: WithClause,
): UpdateRowsWrite<TTable> => {
  if (!by.length) throw new Error("[zodbase] An update of rows needs at least one key field");
  const keys = by.map((field) => String(field.key));
  // A row's key only finds it, as a condition would: stored rows may hold keys their schema no
  // longer accepts. Only the values it writes are parsed.
  const parsed = rows.map((row) => {
    const values = { ...(row as Record<string, unknown>) };
    const found: Record<string, unknown> = {};
    for (const key of keys) {
      if (values[key] === undefined) throw new Error(`[zodbase] Each row of an update by ${key} needs its ${key}`);
      found[key] = values[key];
      delete values[key];
    }
    // Each row's values are sent as a table of literals, which can't name the row's own columns.
    for (const [key, value] of Object.entries(values))
      if (isFieldReference(value)) throw new Error(`[zodbase] A row of an update by key can't set ${key} to a column`);
    return { ...found, ...(Object.keys(values).length ? parseUpdateValues(table, values) : {}) };
  });
  for (const key of incomingFields(where))
    for (const row of parsed)
      if (row[key] === undefined)
        throw new Error(`[zodbase] Each row of an update comparing with incoming(${key}) needs its ${key}`);
  const plan = (keys: readonly string[] | undefined): WritePlan => ({
    empty: !parsed.length,
    async run(target) {
      if (!parsed.length) return EMPTY;
      const result = await target.executeUpdateMany(table, parsed as any, by as any, where && snapshotCondition(where));
      if (keys === undefined) return EMPTY;
      if (result.results.length) return pickKeys(result, keys);
      return pickKeys(
        await target.executeSelect({
          table,
          fields: [{ table, key: "*" } as any],
          where: parsed.map((row) => keyCondition(by, row)).reduce((any, condition) => any.or(condition)),
          orderBy: [],
          limit: undefined,
          offset: undefined,
        }),
        keys,
      );
    },
    build: () => undefined,
  });
  return write(
    adaptor,
    plan(undefined),
    {
      where: (next: SelectCondition) => createUpdateRows(adaptor, table, rows, by, conditionAnd(where, next), clause),
      ...(where
        ? {}
        : { returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}, clause) }),
    },
    clause,
  ) as unknown as UpdateRowsWrite<TTable>;
};

export interface DeleteWrite<TTable extends Table> extends Write {
  /** Narrows the rows it deletes; several calls must all hold. */
  where(condition: SelectCondition<ValueOfTable<TTable>>): DeleteWrite<TTable>;
  /**
   * Runs the delete, returning the rows it deleted (just `keys` of each, when given), as one
   * statement: PostgreSQL and CockroachDB only, such as in a `db.with(…)` whose statement adds up
   * the rows it removed.
   */
  returning(...keys: BindingKeys<ValueOfTable<TTable>>[]): Write<SqlDefiniteResult<ValueOfTable<TTable>, number>>;
}

export const createDelete = <TTable extends Table>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  where?: SelectCondition,
  clause?: WithClause,
): DeleteWrite<TTable> => {
  // Checked when it runs: an invalid condition rejects the delete, before any SQL.
  const condition = where;
  const plan = (keys: readonly string[] | undefined): WritePlan => {
    const returning = keys !== undefined;
    const build = (target: DatabaseAdaptor) => {
      const statement = target.buildDeleteStatement(
        table,
        snapshotCondition(requireWhere("Delete", condition)),
        returning,
      );
      return (
        statement && {
          statement,
          read: (result: SqlResult) => (returning ? pickKeys(target.decode(table, result), keys) : noRows(result)),
        }
      );
    };
    return {
      async run(target) {
        if (returning) {
          const built = build(target);
          if (!built) throw new Error("[zodbase] This database can't return the rows a delete removed");
          return built.read(await target.execute(built.statement));
        }
        await target.executeDelete(table, snapshotCondition(requireWhere("Delete", condition)) as any);
        return EMPTY;
      },
      build,
    };
  };
  return write(
    adaptor,
    plan(undefined),
    {
      where: (next: SelectCondition) => createDelete(adaptor, table, conditionAnd(condition, next), clause),
      returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}, clause),
    },
    clause,
  ) as unknown as DeleteWrite<TTable>;
};

/** The result `Database.batch` gives each write: what awaiting it alone would. */
export type WriteResult<TWrite> = TWrite extends Write<infer TResult> ? Awaited<TResult> : never;

/**
 * Runs `writes` atomically: in one round trip as one statement batch where the adaptor can build
 * every write (`executeTogether`), else one by one in a transaction. A write's own read-back (on
 * adaptors that can't return rows) follows the batch.
 */
export const runBatch = async (adaptor: DatabaseAdaptor, writes: readonly Write<any>[]): Promise<SqlResult[]> => {
  if (!writes.length) return [];
  const plans = writes.map((item) => item[WRITE]);
  const built = plans.map((plan) => plan.build(adaptor));
  if (built.every(Boolean)) {
    const results = await adaptor.executeTogether(built.map((entry) => entry!.statement));
    if (results) return results.map((result, index) => built[index]!.read(result));
  }
  return adaptor.transaction(async (transaction) => {
    const results: SqlResult[] = [];
    for (const plan of plans) results.push(await plan.run(transaction));
    return results;
  });
};

/** Whether a value is a write builder (`Database.batch` takes only these). */
export const isWrite = (value: unknown): value is Write<any> =>
  typeof value === "object" && value !== null && WRITE in value;
