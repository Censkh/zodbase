import { mapSqlResult, normalizeForeignKeyAction, sql, type Table, type TableColumnInfo } from "../../index";
import type { SqlResult } from "../../QueryBuilder";
import PostgresAdaptor from "../postgres";

export default class CockroachAdaptor extends PostgresAdaptor {
  async fetchTableColumns(table: Table): Promise<SqlResult<TableColumnInfo>> {
    const columnResult = await this.execute(sql`
      SHOW
      COLUMNS FROM
      ${table.id}
    `);
    const indexResult = await this.execute(sql`
      SHOW
      INDEX FROM
      ${table.id}
    `);
    const constraintResult = await this.execute(sql`
      SHOW
      CONSTRAINTS FROM
      ${table.id}
    `);
    const foreignKeyResult = await this.execute(sql`
      SELECT
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
      WHERE tc.table_name = ${String(table.id)}
        AND tc.table_schema = current_schema()
        AND tc.constraint_type = 'FOREIGN KEY'
    `);
    const foreignKeys = new Map(
      foreignKeyResult.results.map((row: any) => [
        row.column_name,
        {
          table: row.foreign_table_name,
          field: row.foreign_column_name,
          onDelete: normalizeForeignKeyAction(row.delete_rule),
          constraintName: row.constraint_name,
        },
      ]),
    );

    const primaryKeyIndexes = new Set(
      constraintResult.results
        .filter((constraint) => constraint.constraint_type === "PRIMARY KEY")
        .map((constraint) => constraint.constraint_name),
    );

    const primaryKeyColumns = new Set<string>();
    for (const index of indexResult.results) {
      if (primaryKeyIndexes.has(index.index_name) && !index.storing && !index.implicit) {
        primaryKeyColumns.add(index.column_name);
      }
    }

    return mapSqlResult<any, TableColumnInfo, number>(columnResult, (row) => {
      if (row.is_hidden) {
        return;
      }

      const foreignKey = foreignKeys.get(row.column_name) as TableColumnInfo["foreignKey"];
      return {
        name: row.column_name,
        type: {} as any,
        notNull: !row.is_nullable,
        hasDefault: row.column_default !== null,
        isIdentity: undefined,
        primaryKey: primaryKeyColumns.has(row.column_name),
        ...(foreignKey ? { foreignKey } : {}),
      };
    });
  }
}
