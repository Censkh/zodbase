import type * as zod from "zod";
import type { SelectDialect } from "../../RelationalQuery";
import MysqlAdaptor from "../mysql";

/** MariaDB 10.5+: ordered JSON_ARRAYAGG supports pagination without correlated derived tables. */
export default class MariaDbAdaptor extends MysqlAdaptor {
  protected override selectDialect: SelectDialect = "mariadb";
  // MariaDB's binary collation that doesn't pad: it names it differently.
  protected override textCollation = "utf8mb4_nopad_bin";

  // MariaDB's JSON is text in a collation of its own, which no expression may combine with the
  // tables' other text, so JSON is stored as text in theirs. zod validates it.
  override typeToSql(type: zod.ZodType<any>): string {
    const sqlType = super.typeToSql(type);
    return sqlType === "JSON" ? `LONGTEXT CHARACTER SET utf8mb4 COLLATE ${this.textCollation}` : sqlType;
  }
}
