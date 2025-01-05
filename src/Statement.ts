// hide in final package
export const TO_SQL_SYMBOL = Symbol("toSql");
export type ToSql = {
  [TO_SQL_SYMBOL]: () => string;
};
export interface Statement extends ToSql {}
