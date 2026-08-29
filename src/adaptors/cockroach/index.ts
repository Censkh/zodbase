import { mapSqlResult, sql, type Table, type TableColumnInfo } from "../../index";
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

      return {
        name: row.column_name,
        type: {} as any,
        notNull: !row.is_nullable,
        hasDefault: row.column_default !== null,
        isIdentity: undefined,
        primaryKey: primaryKeyColumns.has(row.column_name),
      };
    });
  }
}
