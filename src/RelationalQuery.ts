import * as zod from "zod";
import type DatabaseAdaptor from "./DatabaseAdaptor";
import {
  buildConditionSql,
  isFieldReference,
  type Selection,
  type SelectQuery,
  type SingleFieldBinding,
} from "./QueryBuilder";
import { SqlExpression } from "./SqlExpression";
import { TO_SQL_SYMBOL } from "./Statement";
import type { Table } from "./Table";
import { isZodTypeExtends } from "./ZodUtils";

export type SelectDialect = "postgres" | "sqlite" | "mysql" | "mariadb" | "mssql";
export const isRelationalQuery = (query: SelectQuery) =>
  Boolean(query.joins?.length || query.projection || query.includes || query.table.sourceTable);
export const isTable = (value: unknown): value is Table =>
  !!value &&
  typeof value === "object" &&
  TO_SQL_SYMBOL in value &&
  "fields" in value &&
  "schema" in value &&
  "id" in value;

export const compileRelationalQuery = (
  adaptor: DatabaseAdaptor,
  dialect: SelectDialect,
  query: SelectQuery,
  scalar = false,
) => {
  const q = (name: string) => adaptor.quoteIdentifier(name);
  let sequence = 0;
  const reserved = new Set<string>();
  const reserve = (query: SelectQuery) => {
    for (const table of [query.table, ...(query.joins ?? []).map((join) => join.table)]) {
      reserved.add(String(table.id));
      for (const key of Object.keys(table.fields)) reserved.add(key);
    }
    for (const child of Object.values(query.includes ?? {})) reserve(child);
  };
  reserve(query);
  const unique = () => {
    let name: string;
    do {
      name = `_zodbase_${sequence++}`;
    } while (reserved.has(name));
    reserved.add(name);
    return name;
  };
  const tableSource = (table: Table) =>
    `${q(String(table.sourceTable?.id ?? table.id))}${table.sourceTable ? ` AS ${q(String(table.id))}` : ""}`;
  const compile = (
    query: SelectQuery,
    outer: Map<string, Table>,
    nested = false,
  ): {
    sql: string;
    columns: string[];
    decode: (row: Record<string, any>) => any;
    ordinal?: string;
    expressions: string[];
    from: string;
    order: string;
  } => {
    for (const value of [query.limit, query.offset]) {
      if (value !== undefined && (!Number.isSafeInteger(value) || value < 0))
        throw new Error("Invalid limit or offset");
    }
    const tables = new Map(outer);
    const own = [query.table, ...(query.joins ?? []).map((join) => join.table)];
    for (const table of own) {
      const name = String(table.id);
      if (tables.has(name)) throw new Error(`Duplicate table scope '${name}'; use alias()`);
      tables.set(name, table);
    }
    const checkField = (field: SingleFieldBinding) => {
      if (tables.get(String(field.table.id)) !== field.table || field.table.fields[String(field.key)] !== field)
        throw new Error("Field references a table outside the query scope");
    };
    const ref = (field: SingleFieldBinding) => {
      checkField(field);
      return `${q(String(field.table.id))}.${q(String(field.key))}`;
    };
    const condition = (value: NonNullable<SelectQuery["where"]>): string => {
      const visit = (value: NonNullable<SelectQuery["where"]>) => {
        if ("conditions" in value) value.conditions.forEach(visit);
        else {
          checkField(value.field);
          if (isFieldReference(value.value)) checkField(value.value);
        }
      };
      visit(value);
      return buildConditionSql(adaptor, value)[TO_SQL_SYMBOL]();
    };
    const markers = new Map<Table, string>();
    const joins = (query.joins ?? [])
      .map((join) => {
        // A sentinel distinguishes an absent row from a matched row whose selected fields are all NULL.
        if (join.type === "LEFT") {
          const marker = unique();
          markers.set(join.table, marker);
          return `LEFT JOIN (SELECT *, 1 AS ${q(marker)} FROM ${q(String(join.table.sourceTable?.id ?? join.table.id))}) AS ${q(String(join.table.id))} ON ${condition(join.on)}`;
        }
        return `INNER JOIN ${tableSource(join.table)} ON ${condition(join.on)}`;
      })
      .join(" ");
    const expressions: string[] = [];
    const columns: string[] = [];
    const add = (expression: string) => {
      const name = unique();
      expressions.push(`${expression} AS ${q(name)}`);
      columns.push(name);
      return name;
    };
    const projection =
      query.projection ??
      (Object.fromEntries(
        query.fields.flatMap((field) =>
          field.key === "*" ? Object.entries(query.table.fields) : [[String(field.key), field]],
        ),
      ) as Selection);
    const sources = (value: Selection | Table | SingleFieldBinding | SqlExpression): Set<Table> => {
      if (value instanceof SqlExpression) return new Set();
      if (isTable(value)) return new Set([value]);
      if (isFieldReference(value)) return new Set([value.table]);
      return new Set(Object.values(value).flatMap((child) => [...sources(child)]));
    };
    const node = (
      value: Selection | Table | SingleFieldBinding | SqlExpression,
      root = false,
    ): ((row: Record<string, any>) => any) => {
      if (value instanceof SqlExpression || isFieldReference(value)) {
        const source = value instanceof SqlExpression ? value.statement[TO_SQL_SYMBOL]() : ref(value);
        let expression = source;
        if (!scalar && (isZodTypeExtends(value.schema, zod.ZodBigInt) || isZodTypeExtends(value.schema, zod.ZodDate)))
          expression = `CAST(${source} AS ${dialect === "mysql" || dialect === "mariadb" ? "CHAR" : dialect === "mssql" ? "NVARCHAR(MAX)" : "TEXT"})`;
        if (!scalar && dialect === "mssql" && isZodTypeExtends(value.schema, zod.ZodDate))
          expression = `CONVERT(NVARCHAR(30), ${source}, 126)`;
        const key = add(expression);
        return (row) => adaptor.decodeSelectedValue(value.schema, row[key]);
      }
      const object = isTable(value) ? value.fields : value;
      const entries = Object.entries(object).map(([key, child]) => [key, node(child)] as const);
      if (!entries.length) throw new Error("Empty selection objects are not supported");
      const tables = sources(value);
      const source = tables.size === 1 ? [...tables][0] : undefined;
      const marker = source && markers.get(source);
      const presence = !root && marker ? add(`${q(String(source!.id))}.${q(marker)}`) : undefined;
      return (row) =>
        presence && row[presence] == null
          ? null
          : Object.fromEntries(entries.map(([key, decode]) => [key, decode(row)]));
    };
    const decodeProjection = node(projection, true);
    const includes = Object.entries(query.includes ?? {}).map(([key, child]) => {
      // biome-ignore lint/suspicious/noPrototypeBuiltins: Support the package's ES2015 target.
      if (Object.prototype.hasOwnProperty.call(projection, key))
        throw new Error(`Include '${key}' collides with a selected field`);
      const compiled = compile(child, tables, true);
      const alias = unique();
      const jsonObject = `${dialect === "sqlite" ? "json_object" : "JSON_OBJECT"}(${compiled.columns.flatMap((column) => [adaptor.valueToSql(column), `${q(alias)}.${q(column)}`]).join(", ")})`;
      let expression: string;
      if (dialect === "postgres")
        expression = `(SELECT COALESCE(json_agg(row_to_json(${q(alias)})), '[]'::json) FROM (${compiled.sql}) AS ${q(alias)})`;
      else if (dialect === "mariadb") {
        const object = `JSON_OBJECT(${compiled.columns.flatMap((column, i) => [adaptor.valueToSql(column), compiled.expressions[i]!]).join(", ")})`;
        const pagination =
          child.limit !== undefined || child.offset !== undefined
            ? ` LIMIT ${child.limit ?? "18446744073709551615"}${child.offset !== undefined ? ` OFFSET ${child.offset}` : ""}`
            : "";
        expression = `(SELECT COALESCE(JSON_ARRAYAGG(${object}${compiled.order ? ` ORDER BY ${compiled.order}` : ""}${pagination}), JSON_ARRAY()) ${compiled.from})`;
      } else if (dialect === "mssql") expression = `(${compiled.sql} FOR JSON PATH, INCLUDE_NULL_VALUES)`;
      else
        expression = `(SELECT ${dialect === "sqlite" ? `json_group_array(${jsonObject})` : `COALESCE(JSON_ARRAYAGG(${jsonObject}), JSON_ARRAY())`} FROM (${compiled.sql}) AS ${q(alias)})`;
      const column = add(expression);
      return { key, column, compiled };
    });
    const order = query.orderBy
      .map(({ field, direction }) => {
        if (direction !== "ASC" && direction !== "DESC") throw new Error("Invalid order direction");
        return `${ref(field as SingleFieldBinding)} ${direction}`;
      })
      .join(", ");
    // Preserve collection ordering even on databases whose JSON aggregate ignores input order.
    const ordinal = nested && order && dialect !== "mariadb" ? add(`ROW_NUMBER() OVER (ORDER BY ${order})`) : undefined;
    const useOffset =
      dialect === "mssql" &&
      (query.offset !== undefined || (nested && !!order && query.limit === undefined)) &&
      query.limit !== 0;
    const top = dialect === "mssql" && !useOffset && query.limit !== undefined ? `TOP (${query.limit}) ` : "";
    let suffix = order || useOffset ? ` ORDER BY ${order || "(SELECT NULL)"}` : "";
    if (dialect === "mssql") {
      if (useOffset)
        suffix += ` OFFSET ${query.offset ?? 0} ROWS${query.limit !== undefined ? ` FETCH NEXT ${query.limit} ROWS ONLY` : ""}`;
    } else {
      if (query.limit !== undefined) suffix += ` LIMIT ${query.limit}`;
      else if (query.offset !== undefined && dialect !== "postgres")
        suffix += ` LIMIT ${dialect === "sqlite" ? "-1" : "18446744073709551615"}`;
      if (query.offset !== undefined) suffix += ` OFFSET ${query.offset}`;
    }
    const from = `FROM ${tableSource(query.table)} ${joins}${query.where ? ` WHERE ${condition(query.where)}` : ""}`;
    return {
      sql: `SELECT ${top}${expressions.join(", ")} ${from}${suffix}`,
      columns,
      ordinal,
      from,
      order,
      expressions: expressions.map((expression, i) => expression.slice(0, -` AS ${q(columns[i]!)}`.length)),
      decode: (row) => {
        const result = decodeProjection(row);
        for (const { key, column, compiled } of includes) {
          let children = row[column] ?? [];
          if (typeof children === "string") children = JSON.parse(children);
          if (!Array.isArray(children)) throw new Error("Invalid included collection result");
          if (compiled.ordinal) children.sort((a, b) => Number(a[compiled.ordinal!]) - Number(b[compiled.ordinal!]));
          Object.defineProperty(result, key, {
            value: children.map(compiled.decode),
            enumerable: true,
            configurable: true,
            writable: true,
          });
        }
        return result;
      },
    };
  };
  return compile(query, new Map());
};
