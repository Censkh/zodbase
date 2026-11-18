import type * as zod from "zod";
import { getMetaItem, type ZodMetaItem } from "zod-meta";
import {
  type BackfillOptions,
  backfill,
  computed,
  type FieldDiffType,
  isZodRequired,
  normalizeForeignKeyAction,
  raw,
  sql,
  type Table,
  type TableColumnInfo,
  type TableDiff,
  valueToSql,
} from "../..";
import DatabaseAdaptor, {
  type Conflict,
  groupUpsertRows,
  normalizeIndexDefinition,
  type PossiblySelectedResult,
} from "../../DatabaseAdaptor";
import { quoteIdentifier } from "../../Escaping";
import {
  buildConditionSql,
  type InputOfTable,
  SELECT_QUERY,
  type SelectCondition,
  type SelectQuery,
  type SingleFieldBinding,
  type SqlDefiniteResult,
  type SqlResult,
  type StringKeys,
  type ValueOfTable,
} from "../../QueryBuilder";
import { isRelationalQuery } from "../../RelationalQuery";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import { isSubquery } from "../../Subquery";
import { type LiveTableSchema, type TableIndex, writableFieldKeys } from "../../Table";
import { spliceNulCharacters } from "../SqlText";
import { caseInsensitiveGlob } from "./CaseFolding";

type BackfillMetaItem = ZodMetaItem<BackfillOptions>;

/**
 * The tables in SQLite's PRAGMA rows, each row naming its table: one parse for a read of every table
 * and of one alike.
 */
const parseSqliteSchema = (
  columnRows: Record<string, unknown>[],
  foreignKeyRows: Record<string, unknown>[],
  indexRows: Record<string, unknown>[],
): Map<string, LiveTableSchema> => {
  const schema = new Map<string, LiveTableSchema>();
  const tableOf = (row: Record<string, unknown>) => {
    const name = String(row._zodbase_table);
    if (!schema.has(name)) schema.set(name, { columns: [], indexes: [] });
    return schema.get(name)!;
  };
  const foreignKeys = new Map(
    foreignKeyRows.map((row) => [
      `${row._zodbase_table}\u0000${row.from}`,
      { table: String(row.table), field: String(row.to), onDelete: normalizeForeignKeyAction(String(row.on_delete)) },
    ]),
  );
  for (const row of columnRows) {
    const table = tableOf(row);
    // Hidden 1 is a virtual table's hidden column; 2 and 3 are generated columns.
    if (Number(row.hidden) === 1) continue;
    const foreignKey = foreignKeys.get(`${row._zodbase_table}\u0000${row.name}`) as TableColumnInfo["foreignKey"];
    table.columns.push({
      name: String(row.name),
      type: {} as any,
      notNull: Number(row.notnull) === 1,
      // pk is the column's position in the primary key, from 1; 0 for other columns.
      primaryKey: Number(row.pk) > 0,
      generated: Number(row.hidden) >= 2,
      ...(foreignKey ? { foreignKey } : {}),
    });
  }
  for (const row of indexRows) {
    // A primary key's autoindex belongs to the table; UNIQUE constraints ("u") and created indexes
    // ("c") are reported.
    if (row.origin === "pk" || !schema.has(String(row._zodbase_table))) continue;
    const table = tableOf(row);
    const name = String(row.name);
    let index = table.indexes.find((candidate) => candidate.name === name);
    if (!index) {
      index = {
        name,
        unique: Number(row.unique) === 1,
        columns: [],
        partial: Number(row.partial) === 1,
        ...(typeof row.sql === "string" ? { definition: normalizeIndexDefinition(row.sql) } : {}),
      };
      table.indexes.push(index);
    }
    if (row.column !== null && row.column !== undefined) index.columns.push(String(row.column));
  }
  return schema;
};

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

export default abstract class SqliteAdaptor<TDriver> extends DatabaseAdaptor<TDriver> {
  // SQLite's columns convert what they store by their affinity: a cast would turn `true` into 0.
  protected override insertSelectValueSql(value: string): string {
    return value;
  }

  override async createTable(table: Table, name?: string) {
    await this.execute(raw("PRAGMA foreign_keys = ON"));
    return super.createTable(table, name);
  }

  buildJsonArrayContainsSql(fieldSql: string, value: unknown): Statement {
    return sql`EXISTS (SELECT 1 FROM json_each(${raw(fieldSql)}) WHERE value = ${value})`;
  }

  async processDiff(table: Table, diff: TableDiff): Promise<void> {
    let remake = false;
    let tableHasRows: boolean | undefined;

    for (const fieldDiff of diff.fields) {
      const schema = fieldDiff.field?.schema;
      // SQLite can't add a stored generated column: the table is made again with it, filled in.
      if (fieldDiff.type === "added" && schema && getMetaItem(schema, computed)) {
        remake = true;
        continue;
      }
      const addsRequiredField = fieldDiff.type === "added" && schema && isZodRequired(schema);
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
        if (!schema || getMetaItem(schema, computed)) {
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
          remake = true;
        }
        if (isZodRequired(schema)) {
          remake = true;
        }
      } else if (fieldDiff.type === "removed") {
        await this.dropIndexesReferencingColumn(table, String(fieldDiff.key));
        await this.execute(sql`ALTER TABLE ${table.id}
            DROP COLUMN ${raw(quoteIdentifier(String(fieldDiff.key)))}`);
      } else if (fieldDiff.type === "modified") {
        remake = true;
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
        }
      }
    }

    // A primary key can't change in place either: the rebuilt table declares the new one.
    if (diff.primaryKey) remake = true;
    if (remake) {
      // sqlite does not support modifying columns, so we need to create a new table
      const tempTableId = `${table.id}_temp_${crypto.randomUUID().split("-")[0]}`;
      await this.createTable(table, tempTableId);
      // Computed columns are the new table's own, and an existing table may not have them yet.
      const columns = writableFieldKeys(table).map(quoteIdentifier).join(", ");
      await this.execute(
        sql`INSERT INTO ${raw(quoteIdentifier(tempTableId))} (${raw(columns)})
            SELECT ${raw(columns)} FROM ${table}`,
      );
      await this.execute(sql`DROP TABLE ${table.id}`);
      await this.execute(sql`ALTER TABLE ${raw(quoteIdentifier(tempTableId))} RENAME TO ${table}`);
    }
  }

  // SQLite refuses to drop a column while an index still uses it, and the table definition no longer
  // knows about indexes that were removed along with the column. Drop every explicitly created index
  // that lists the column or mentions it in its partial predicate; indexes the table still declares
  // are recreated by syncTableIndexes afterwards.
  private async dropIndexesReferencingColumn(table: Table, column: string): Promise<void> {
    const indexes = await this.execute(sql`PRAGMA index_list(${raw(quoteIdentifier(table.id))})`);
    for (const index of indexes.results as { name: string; origin: string }[]) {
      if (index.origin !== "c") continue;
      const columns = await this.execute(sql`PRAGMA index_info(${raw(quoteIdentifier(index.name))})`);
      const listsColumn = (columns.results as { name: string | null }[]).some((info) => info.name === column);
      let mentionsColumn = false;
      if (!listsColumn) {
        const definition = await this.execute(
          sql`SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ${index.name}`,
        );
        const definitionSql = String((definition.first as { sql?: string | null } | undefined)?.sql ?? "");
        const whereAt = definitionSql.toUpperCase().lastIndexOf(" WHERE ");
        if (whereAt !== -1) {
          const predicate = definitionSql.slice(whereAt);
          const escapedColumn = column.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
          mentionsColumn =
            predicate.includes(quoteIdentifier(column)) ||
            new RegExp(`(^|[^\\w"])${escapedColumn}([^\\w"]|$)`).test(predicate);
        }
      }
      if (listsColumn || mentionsColumn) {
        await this.execute(sql`DROP INDEX IF EXISTS ${raw(quoteIdentifier(index.name))}`);
      }
    }
  }

  createIndexStatement(table: Table, index: TableIndex): Statement {
    return sql`
        CREATE ${raw(index.unique ? "UNIQUE " : "")}INDEX IF NOT EXISTS ${raw(quoteIdentifier(index.id))}
          ON ${table.id} (${raw(index.fields.map((field) => quoteIdentifier(String(field.key))).join(", "))})
          ${index.where ? sql`WHERE ${buildConditionSql(this, index.where, { includeTable: false })}` : raw("")}
      `;
  }

  protected mapResult(value: SqlResult): SqlResult {
    return value;
  }

  protected override selectDialect = "sqlite" as const;

  /**
   * Rows as one multi-row VALUES, its columns named: SQLite refuses a compound SELECT of more than
   * 500 terms, so a UNION ALL of one SELECT per row can't hold more rows than that.
   */
  private valuesSql(columns: readonly string[], rows: readonly Record<string, unknown>[]): string {
    const values = rows.map(
      (row) =>
        `(${columns
          .map((column) => {
            const value = row[column];
            return isSubquery(value)
              ? `(${this.buildSelectSql(value[SELECT_QUERY].query, true)[TO_SQL_SYMBOL]()})`
              : this.valueToSql(value, true);
          })
          .join(", ")})`,
    );
    const named = columns.map(
      (column, index) => `${this.quoteIdentifier(`column${index + 1}`)} AS ${this.quoteIdentifier(column)}`,
    );
    return `SELECT ${named.join(", ")} FROM (VALUES ${values.join(", ")})`;
  }

  override rowsSourceSql(table: Table, rows: readonly Record<string, unknown>[]): string {
    if (!rows.length) return super.rowsSourceSql(table, rows);
    return `(${this.valuesSql(Object.keys(table.fields), rows)})`;
  }

  override buildConditionalInsertSql(
    table: Table,
    rows: readonly Record<string, unknown>[],
    condition: SelectCondition | undefined,
    shouldReturn: boolean,
    conflict?: Conflict,
  ): Statement {
    const columns = writableFieldKeys(table);
    const where = condition ? ` WHERE ${buildConditionSql(this, condition, true)[TO_SQL_SYMBOL]()}` : "";
    return this.buildInsertSelectSql(
      table,
      columns,
      raw(`${this.valuesSql(columns, rows)}${where}`),
      shouldReturn,
      conflict,
    );
  }

  // SQLite reads a statement's text only up to a NUL character, but stores one in a string.
  protected override sqlText(statement: Statement): string {
    return spliceNulCharacters(super.sqlText(statement), "char(0)");
  }

  // SQLite's LIKE and LOWER fold only ASCII letters, so these match the lowered value through GLOB
  // character classes for the rest.
  override buildPatternMatchSql(fieldSql: string, pattern: string, negate: boolean): Statement {
    return sql`LOWER(${raw(fieldSql)}) ${raw(negate ? "NOT GLOB" : "GLOB")} ${caseInsensitiveGlob(pattern, true)}`;
  }

  /** The longest LIKE or GLOB pattern the database takes, in bytes: SQLite's default. */
  protected patternLengthLimit = 50_000;

  // instr has no limit on what it looks for, and treats every character literally, so a search is
  // a GLOB only where it has letters beyond ASCII to fold: LOWER folds only ASCII ones. A search too
  // long for the database's pattern limit folds its ASCII letters alone, and matches others exactly.
  override buildContainsTextSql(fieldSql: string, text: string): Statement {
    const value = sql`LOWER(${raw(fieldSql)})`;
    const lowered = Array.from(text, (char) => (char <= "\u007f" ? char.toLowerCase() : char)).join("");
    // biome-ignore lint/suspicious/noControlCharactersInRegex: ASCII is every character from NUL.
    if (/^[\u0000-\u007f]*$/.test(text)) return sql`instr(${value}, ${lowered}) > 0`;
    const glob = `*${caseInsensitiveGlob(text, false)}*`;
    if (new TextEncoder().encode(glob).length <= this.patternLengthLimit) return sql`${value} GLOB ${glob}`;
    return sql`instr(${value}, ${lowered}) > 0`;
  }

  override buildSelectSql(select: SelectQuery, scalar = false, named = false): Statement {
    if (isRelationalQuery(select)) return this.buildRelationalSelectSql(select, scalar, named);
    return sql`SELECT ${raw(scalar ? select.fields.map((field) => this.quoteIdentifier(String(field.key))).join(", ") : this.selectFields(select.table, select.fields))}
            FROM ${select.table} ${select.where ? sql` WHERE ${buildConditionSql(this, select.where)}` : raw("")}${
              select.orderBy.length > 0
                ? sql` ORDER BY ${raw(
                    select.orderBy.map(
                      (order) =>
                        `${this.quoteIdentifier(String(select.table.id))}.${this.quoteIdentifier(String(order.field.key))} ${order.direction}`,
                    ),
                  )}`
                : raw("")
            }${raw(select.limit !== undefined ? ` LIMIT ${select.limit}` : select.offset !== undefined ? " LIMIT -1" : "")}${raw(
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
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, 1>> {
    const statement = sql`INSERT INTO ${table.id} (${raw(Object.keys(values as any).map(quoteIdentifier))})
                 VALUES ${Object.values(values as any)}`;
    await this.execute(statement);
    return {
      first: values as any,
      results: [values as any],
    };
  }

  async executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
  ): Promise<SqlDefiniteResult<ValueOfTable<TTable>, number>> {
    const fieldKeys = writableFieldKeys(table);
    const statement = sql`INSERT INTO ${table.id} (${raw(fieldKeys.map(quoteIdentifier))})
                 VALUES ${raw(values.map((value: any) => sql`${fieldKeys.map((key) => value[key])}`))}`;
    await this.execute(statement);
    return {
      first: values[0] as any,
      results: values as any,
    };
  }

  /** Runs read-only statements, in one round trip where the driver can batch them. */
  protected async executeReads(statements: Statement[]): Promise<SqlResult[]> {
    const results: SqlResult[] = [];
    for (const statement of statements) results.push(await this.execute(statement));
    return results;
  }

  /**
   * Three schema-wide reads, together (executeReads): one round trip on D1 and libSQL. The PRAGMAs'
   * table-valued forms read every table's columns, foreign keys and indexes at once, where the
   * PRAGMA statements read one table each.
   */
  override async fetchLiveSchema(tables?: readonly Table[]): Promise<Map<string, LiveTableSchema>> {
    const names = tables?.map((table) => String(table.id));
    const only = names ? ` AND m.name IN (${names.map((name) => this.valueToSql(name)).join(", ")})` : "";
    if (names && !names.length) return new Map();
    const from = `FROM sqlite_master m`;
    const where = `WHERE m.type = 'table'${only}`;
    const [columns, foreignKeys, indexes] = await this.executeReads([
      // table_xinfo, unlike table_info, lists generated columns (hidden 2 or 3).
      raw(
        `SELECT m.name AS "_zodbase_table", c.* ${from}, pragma_table_xinfo(m.name) c ${where} ORDER BY m.name, c.cid`,
      ),
      raw(`SELECT m.name AS "_zodbase_table", f.* ${from}, pragma_foreign_key_list(m.name) f ${where}`),
      raw(
        `SELECT m.name AS "_zodbase_table", l.name, l."unique", l.origin, l.partial, i.seqno, i.name AS "column", s.sql ${from}, pragma_index_list(m.name) l LEFT JOIN pragma_index_info(l.name) i LEFT JOIN sqlite_master s ON s.type = 'index' AND s.name = l.name ${where} ORDER BY m.name, l.name, i.seqno`,
      ),
    ]);
    return parseSqliteSchema(
      (columns!.results as Record<string, unknown>[]).filter(
        (row) => !this.isInternalTable(String(row._zodbase_table)),
      ),
      foreignKeys!.results as Record<string, unknown>[],
      indexes!.results as Record<string, unknown>[],
    );
  }

  /** Tables the engine keeps for itself, such as `sqlite_sequence`. */
  protected isInternalTable(name: string): boolean {
    return name.startsWith("sqlite_");
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

  async executeUpsert<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    conflict: Conflict,
  ): Promise<SqlResult<void, 0>> {
    const sql = this.buildUpsertSql(table, values as any, conflict);
    return (await this.execute(sql)) as any;
  }

  override async executeUpsertMany<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>[],
    conflict: Conflict,
  ): Promise<SqlResult<void, 0>> {
    for (const rows of groupUpsertRows(
      values as Record<string, unknown>[],
      conflict.fields.map((field) => String(field.key)),
    )) {
      const keys = Object.keys(rows[0]!);
      await this.execute(sql`INSERT INTO ${table.id} (${raw(keys.map(quoteIdentifier))})
                 VALUES ${raw(rows.map((row) => sql`${keys.map((key) => row[key])}`))}${raw(this.onConflictSql(conflict, keys))}`);
    }
    return { results: [], first: undefined };
  }

  // SQLite reads `INSERT … SELECT … ON CONFLICT` as a join's ON unless the select has a WHERE.
  protected override insertSelectSource(select: Statement, conflict?: Conflict): Statement {
    return conflict ? sql`SELECT * FROM (${select}) WHERE true` : select;
  }

  executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
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
                          ${buildConditionSql(this, where)}`
                              : ""
                          }`;
    return this.execute(statement) as any;
  }

  // SQLite returns the rows a delete removes, as PostgreSQL does.
  override buildDeleteStatement(table: Table, where: SelectCondition, returning = false): Statement {
    return sql`DELETE FROM ${table} WHERE ${buildConditionSql(this, where, true)}${raw(returning ? ` RETURNING ${this.selectFields(table)}` : "")}`;
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
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
    shouldReturn = false,
  ): Statement {
    return sql`UPDATE ${table}
                 SET ${raw(
                   Object.entries(values).reduce((acc, [key, value]) => {
                     if (value !== undefined) {
                       acc.push(raw(`${quoteIdentifier(key)} = ${this.valueToSql(value, true)}`));
                     }
                     return acc;
                   }, [] as Statement[]),
                 )}
                 WHERE ${buildConditionSql(this, where)}${raw(shouldReturn ? ` RETURNING ${this.selectFields(table)}` : "")}`;
  }

  protected buildUpsertSql<TTable extends Table>(
    table: TTable,
    values: Partial<ValueOfTable<TTable>>,
    conflict: Conflict,
  ): Statement {
    return sql`INSERT INTO ${table.id} (${raw(Object.keys(values).map(quoteIdentifier))})
                 VALUES ${Object.values(values)}${raw(this.onConflictSql(conflict, Object.keys(values)))}`;
  }
}
