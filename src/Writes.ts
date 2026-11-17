import type * as zod from "zod";
import { findFieldMetaItems } from "zod-meta";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import { keyCondition } from "./DatabaseAdaptor";
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
import { isScalarSelect, SELECT_QUERY } from "./QueryBuilder";
import { COMPILE, type Compilable, toQueryStatement } from "./QueryStatement";
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

/** Wraps a plan as an awaitable `Write` with `extra` members. */
const write = <TResult extends SqlResult, TExtra extends object>(
  adaptor: () => DatabaseAdaptor,
  plan: WritePlan,
  extra: TExtra,
): Write<TResult> & TExtra =>
  toLazyPromise(() => plan.run(adaptor()) as Promise<TResult>, {
    ...extra,
    [WRITE]: plan,
    [COMPILE]: (target: DatabaseAdaptor) => {
      const built = plan.build(target);
      if (!built) throw new Error("[zodbase] This database can't compile this write as a statement");
      return built.statement;
    },
  }) as unknown as Write<TResult> & TExtra;

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
    parsed[key] = value instanceof SqlExpression ? value : schema.shape[key].parse(value);
  }
  for (const field of findFieldMetaItems(table.schema, updatedAt)) parsed[field.key] = Date.now();
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

type Conflict = { fields: readonly SingleFieldBinding[]; action: "nothing" | "update" };

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
   * fields of a unique key in order): skip it (`doNothing`), or update the existing row to it
   * (`doUpdate`). With `doNothing`, `returning` gives only the rows inserted.
   */
  onConflict(key: SingleFieldBinding<ValueOfTable<TTable>> | readonly SingleFieldBinding<ValueOfTable<TTable>>[]): {
    doNothing(): InsertWrite<TTable, TOne>;
    doUpdate(): InsertWrite<TTable, TOne>;
  };
}

/** An insert of values some of which are subqueries, run by the database: no parsed rows or conflicts. */
export type SubqueryInsertWrite<TTable extends Table, TOne extends boolean> = Omit<
  InsertWrite<TTable, TOne>,
  "rows" | "onConflict"
>;

export const createInsert = <TTable extends Table, TOne extends boolean>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  input: InsertValues<TTable>[],
  one: TOne,
  conflict?: Conflict,
): InsertWrite<TTable, TOne> => {
  const subqueries = hasSubqueries(input as object[]);
  if (subqueries && conflict) throw new Error("[zodbase] An insert of subquery values can't handle conflicts");
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
    return {
      async run(target) {
        if (!rows.length) return EMPTY;
        if (subqueries) return read(await target.executeInsertSubqueries(table, stored as any, returning));
        if (conflict?.action === "nothing")
          return read(await target.executeInsertIgnoring(table, stored as any, conflict.fields, returning));
        if (conflict?.action === "update") {
          if (rows.length === 1) {
            const result = await target.executeUpsert(table, stored[0] as any, conflict.fields as any);
            if (!returning) return EMPTY;
            if (result.results.length) return read(result);
            // Adaptors that can't return an upsert's row read it back by its key.
            const row = rows[0]!;
            const where = conflict.fields
              .map((field) => field.equals(row[String(field.key)] as any) as SelectCondition)
              .reduce((all, condition) => all.and(condition));
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
          await target.executeUpsertMany(table, stored as any, conflict.fields as any);
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
        if (subqueries || !rows.length) return undefined;
        const statement = target.buildInsertStatement(table, stored, { returning, conflict });
        if (!statement) return undefined;
        return { statement, read: (result) => (returning ? read(target.decode(table, result)) : noRows(result)) };
      },
    };
  };

  return write(adaptor, plan(undefined), {
    rows: rows as ValueOfTable<TTable>[],
    returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}),
    onConflict: (key: SingleFieldBinding | readonly SingleFieldBinding[]) => ({
      doNothing: () => createInsert(adaptor, table, input, one, { fields: keyFields(key), action: "nothing" }),
      doUpdate: () => createInsert(adaptor, table, input, one, { fields: keyFields(key), action: "update" }),
    }),
  }) as unknown as InsertWrite<TTable, TOne>;
};

export interface InsertSelectWrite<TTable extends Table> extends Write {
  /** Runs the insert, returning the rows it wrote (just `keys` of each, when given). */
  returning(...keys: BindingKeys<ValueOfTable<TTable>>[]): Write<SqlResult<ValueOfTable<TTable>>>;
}

/** `INSERT INTO table (columns) <select>`: the select's projection names the columns. */
export const createInsertSelect = <TTable extends Table>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  select: Compilable & { [SELECT_QUERY]: { query: { projection?: object } } },
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
      const statement = target.buildInsertSelectSql(table, columns, source.compile(target), returning);
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
  return write(adaptor, plan(undefined), {
    returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}),
  }) as unknown as InsertSelectWrite<TTable>;
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
  return write(adaptor, plan(undefined), {
    where: (next: SelectCondition) => createUpdate(adaptor, table, values, conditionAnd(condition, next)),
    returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}),
  }) as unknown as UpdateWrite<TTable>;
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
  /** Runs the update, returning the rows it changed (just `keys` of each, when given). */
  returning(...keys: BindingKeys<ValueOfTable<TTable>>[]): Write<SqlDefiniteResult<ValueOfTable<TTable>, number>>;
}

/** Updates each of `rows` found by its `by` fields to its other values. */
export const createUpdateRows = <TTable extends Table>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  rows: Partial<InputOfTable<TTable>>[],
  by: readonly SingleFieldBinding<ValueOfTable<TTable>>[],
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
    return { ...found, ...(Object.keys(values).length ? parseUpdateValues(table, values) : {}) };
  });
  const plan = (keys: readonly string[] | undefined): WritePlan => ({
    async run(target) {
      if (!parsed.length) return EMPTY;
      const result = await target.executeUpdateMany(table, parsed as any, by as any);
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
  return write(adaptor, plan(undefined), {
    returning: (...keys: string[]) => write(adaptor, plan(keys.length ? keys : ["*"]), {}),
  }) as unknown as UpdateRowsWrite<TTable>;
};

export interface DeleteWrite<TTable extends Table> extends Write {
  /** Narrows the rows it deletes; several calls must all hold. */
  where(condition: SelectCondition<ValueOfTable<TTable>>): DeleteWrite<TTable>;
}

export const createDelete = <TTable extends Table>(
  adaptor: () => DatabaseAdaptor,
  table: TTable,
  where?: SelectCondition,
): DeleteWrite<TTable> => {
  // Checked when it runs: an invalid condition rejects the delete, before any SQL.
  const condition = where;
  const plan: WritePlan = {
    async run(target) {
      await target.executeDelete(table, snapshotCondition(requireWhere("Delete", condition)) as any);
      return EMPTY;
    },
    build(target) {
      const statement = target.buildDeleteStatement(table, snapshotCondition(requireWhere("Delete", condition)));
      return statement && { statement, read: noRows };
    },
  };
  return write(adaptor, plan, {
    where: (next: SelectCondition) => createDelete(adaptor, table, conditionAnd(condition, next)),
  }) as unknown as DeleteWrite<TTable>;
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
