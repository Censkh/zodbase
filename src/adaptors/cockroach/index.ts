import type * as pg from "pg";
import { mapSqlResult, sql, type Table, type TableColumnInfo } from "../../index";
import type { SqlResult } from "../../QueryBuilder";
import PostgresAdaptor from "../postgres";

export default class CockroachAdaptor<TDriver extends pg.Client> extends PostgresAdaptor<TDriver> {
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

    const indexColumns: Record<string, any> = {};
    for (const index of indexResult.results) {
      indexColumns[index.column_name] = index;
    }

    return mapSqlResult<any, TableColumnInfo, number>(columnResult, (row) => {
      if (row.is_hidden) {
        return;
      }

      const index = indexColumns[row.column_name];
      const isPrimaryKey = !index?.storing;

      return {
        name: row.column_name,
        type: {} as any,
        notNull: row.is_nullable,
        hasDefault: row.column_default !== null,
        isIdentity: undefined,
        primaryKey: isPrimaryKey,
      };
    });
  }
}
