import type { SelectDialect } from "../../RelationalQuery";
import MysqlAdaptor from "../mysql";

/** MariaDB 10.5+: ordered JSON_ARRAYAGG supports pagination without correlated derived tables. */
export default class MariaDbAdaptor extends MysqlAdaptor {
  protected override selectDialect: SelectDialect = "mariadb";
}
