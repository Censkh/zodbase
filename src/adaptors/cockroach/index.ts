import type * as zod from "zod";
import { getMetaItem } from "zod-meta";
import type DatabaseAdaptor from "../../DatabaseAdaptor";
import { quoteIdentifier } from "../../Escaping";
import { computed, raw, sql, type Table } from "../../index";
import type { SelectQuery } from "../../QueryBuilder";
import { type Statement, TO_SQL_SYMBOL } from "../../Statement";
import type { LiveTableSchema } from "../../Table";
import PostgresAdaptor, {
  addLiveIndexes,
  type LiveIndexRow,
  type PostgresColumnRow,
  parsePostgresColumn,
} from "../postgres";
import { spliceNulCharacters } from "../SqlText";
import { cockroachLocality, cockroachRegion } from "./metadata";

export default class CockroachAdaptor extends PostgresAdaptor {
  // Materialized CTE buffers disable Cockroach's auto-commit fast path for these inserts.
  protected override materializeRepeatedSubqueries = false;

  // Hidden columns are the engine's own: rowid, crdb_region and hash-sharded index shards.
  protected override visibleColumnCondition = "AND is_hidden = 'NO'";

  // CockroachDB reads a statement's text only up to a NUL character, but stores one in a string.
  protected override sqlText(statement: Statement): string {
    return spliceNulCharacters(statement[TO_SQL_SYMBOL](), "chr(0)");
  }

  // CockroachDB sorts NULL as the smallest value, as SQLite and MySQL do.
  protected override sortsNullFirst = true;

  override divideSql(dividend: string, divisor: string): string {
    // Mixed integer, decimal and float operands can make CockroachDB resolve an invalid overload.
    return `(CAST(${dividend} AS FLOAT8) / CAST(${divisor} AS FLOAT8))`;
  }

  override subtractSql(left: string, right: string): string {
    return `(CAST(${left} AS FLOAT8) - CAST(${right} AS FLOAT8))`;
  }

  // FORCE_INDEX alone still scans the whole index when the estimate covers the table, so the hint
  // also rules full scans out: a query the index can't constrain fails rather than reading it all.
  protected override selectSource(select: SelectQuery): Statement {
    return select.index === undefined
      ? super.selectSource(select)
      : sql`${select.table}@{FORCE_INDEX=${raw(quoteIdentifier(select.index))},NO_FULL_SCAN}`;
  }

  // A computed region column is the region type, which its text expression must be cast to.
  protected override computedColumnSql(table: Table, field: { key: unknown; schema: zod.ZodType }): string {
    const options = getMetaItem(field.schema, computed)?.data;
    if (!options || !getMetaItem(field.schema, cockroachRegion)) return super.computedColumnSql(table, field);
    return `AS ((${options.sql((key) => quoteIdentifier(key))})::${this.typeToSql(field.schema)}) STORED`;
  }

  override typeToSql(schema: zod.ZodType): string {
    return getMetaItem(schema, cockroachRegion) ? '"public"."crdb_internal_region"' : super.typeToSql(schema);
  }

  override async syncTableLocality(table: Table, live?: LiveTableSchema): Promise<boolean> {
    const expected = this.tableLocality(table);
    if (!expected) return false;
    const locality =
      live?.locality ??
      (
        await this.execute(
          sql`SELECT locality FROM [SHOW TABLES] WHERE table_name = ${String(table.id)} AND schema_name = current_schema()`,
        )
      ).first?.locality;
    // SHOW TABLES can omit identifier quotes for simple names.
    if (String(locality).replace(/"/g, "") === expected.replace(/"/g, "")) return false;
    await this.execute(this.setLocalityStatement(table, expected));
    return true;
  }

  override tableSettingStatements(table: Table): Statement[] {
    const expected = this.tableLocality(table);
    return expected ? [this.setLocalityStatement(table, expected)] : [];
  }

  private setLocalityStatement(table: Table, locality: string): Statement {
    return sql`ALTER TABLE ${table.id} SET LOCALITY ${raw(locality)}`;
  }

  /** The table's locality clause, such as `GLOBAL`, or undefined when it sets none. */
  private tableLocality(table: Table): string | undefined {
    const locality = getMetaItem(table.schema, cockroachLocality)?.data;
    if (!locality) return undefined;
    let expected: string;
    if (locality.type === "global") expected = "GLOBAL";
    else if (locality.type === "regional-by-table") {
      expected = locality.region
        ? `REGIONAL BY TABLE IN ${quoteIdentifier(locality.region)}`
        : "REGIONAL BY TABLE IN PRIMARY REGION";
    } else {
      const column = locality.regionColumn;
      if (column && (!table.fields[column] || !getMetaItem(table.fields[column].schema, cockroachRegion))) {
        throw new Error(`Regional column '${column}' must have cockroachRegion metadata`);
      }
      expected = `REGIONAL BY ROW${column ? ` AS ${quoteIdentifier(column)}` : ""}`;
    }
    return expected;
  }

  protected override async executeTransaction<TResult>(
    callback: (adaptor: DatabaseAdaptor) => Promise<TResult>,
  ): Promise<TResult> {
    // A serialization failure aborts the entire transaction; retry after ROLLBACK.
    for (let attempt = 0; ; attempt++) {
      try {
        return await super.executeTransaction(callback);
      } catch (error) {
        if (attempt >= 2 || !error || typeof error !== "object" || !("code" in error) || error.code !== "40001") {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 25 * 2 ** attempt + Math.random() * 25));
      }
    }
  }

  protected override async addPrimaryKey(table: Table, key: string): Promise<void> {
    await this.execute(sql`ALTER TABLE ${table.id} ALTER PRIMARY KEY USING COLUMNS (${raw(quoteIdentifier(key))})`);
  }

  /**
   * Four schema-wide reads in one round trip: every visible column, every index's columns
   * (`information_schema.statistics`, which marks the columns the engine adds itself, such as a
   * regional table's `crdb_region`, as implicit), every index's predicate and comment, and every
   * table's locality.
   */
  override async fetchLiveSchema(tables?: readonly Table[]): Promise<Map<string, LiveTableSchema>> {
    if (tables && !tables.length) return new Map();
    const [columns, statistics, indexes, localities] = await this.readTogether([
      raw(`
      SELECT
        c.table_name,
        c.column_name,
        c.data_type,
        c.is_nullable,
        c.column_default,
        c.generation_expression,
        false AS is_primary_key,
        fk.constraint_name AS foreign_key_constraint_name,
        fk.foreign_table_name,
        fk.foreign_column_name,
        fk.delete_rule
      FROM information_schema.columns c
      LEFT JOIN (
        SELECT
          kcu.table_name,
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
        WHERE tc.table_schema = current_schema() AND tc.constraint_type = 'FOREIGN KEY'
      ) fk ON fk.table_name = c.table_name AND fk.column_name = c.column_name
      WHERE c.table_schema = current_schema() AND c.is_hidden = 'NO'${this.tablesCondition("c.table_name", tables)}
      ORDER BY c.table_name, c.ordinal_position`),
      raw(`
      SELECT table_name, index_name, non_unique, column_name
      FROM information_schema.statistics
      WHERE table_schema = current_schema() AND implicit = 'NO' AND storing = 'NO'${this.tablesCondition("table_name", tables)}
      ORDER BY table_name, index_name, seq_in_index`),
      raw(`
      SELECT
        t.relname AS table_name,
        i.relname AS index_name,
        ix.indisprimary AS is_primary,
        ix.indpred IS NOT NULL AS is_partial,
        obj_description(i.oid, 'pg_class') AS comment
      FROM pg_catalog.pg_index ix
      JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
      JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
      JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
      WHERE n.nspname = current_schema()${this.tablesCondition("t.relname", tables)}`),
      raw(
        `SELECT table_name, locality FROM [SHOW TABLES] WHERE schema_name = current_schema()${this.tablesCondition("table_name", tables)}`,
      ),
    ]);
    const details = new Map(
      (indexes!.results as Record<string, unknown>[]).map((row) => [`${row.table_name}\u0000${row.index_name}`, row]),
    );
    const schema = new Map<string, LiveTableSchema>();
    for (const row of columns!.results as PostgresColumnRow[]) {
      if (!schema.has(row.table_name)) schema.set(row.table_name, { columns: [], indexes: [] });
      schema.get(row.table_name)!.columns.push(parsePostgresColumn(row));
    }
    const indexRows: LiveIndexRow[] = [];
    for (const row of statistics!.results as Record<string, string>[]) {
      const detail = details.get(`${row.table_name}\u0000${row.index_name}`);
      const table = schema.get(row.table_name!);
      if (!table) continue;
      // The primary key's columns, without those the engine adds (a regional table's crdb_region).
      if (detail?.is_primary === true) {
        const column = table.columns.find((candidate) => candidate.name === row.column_name);
        if (column) column.primaryKey = true;
        continue;
      }
      indexRows.push({
        table_name: row.table_name!,
        index_name: row.index_name!,
        is_unique: row.non_unique === "NO",
        is_partial: Boolean(detail?.is_partial),
        comment: (detail?.comment as string | null | undefined) ?? null,
        column_name: row.column_name!,
      });
    }
    addLiveIndexes(schema, indexRows);
    for (const row of localities!.results as { table_name: string; locality: string | null }[]) {
      const table = schema.get(row.table_name);
      if (table && row.locality) table.locality = row.locality;
    }
    return schema;
  }

  protected override indexReference(table: Table, name: string): string {
    return `${quoteIdentifier(String(table.id))}@${quoteIdentifier(name)}`;
  }

  // CockroachDB drops a unique index only with CASCADE, which also drops a foreign key that relies
  // on it: syncing the referencing table adds that key again.
  protected override dropIndexStatement(table: Table, name: string): Statement {
    return raw(`DROP INDEX IF EXISTS ${this.indexReference(table, name)} CASCADE`);
  }
}

export { cockroachLocality, cockroachRegion } from "./metadata";
