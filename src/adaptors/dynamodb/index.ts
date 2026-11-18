import {
  type AttributeDefinition,
  CreateTableCommand,
  DescribeTableCommand,
  type DynamoDBClient,
  type GlobalSecondaryIndex,
  ResourceNotFoundException,
  UpdateTableCommand,
  waitUntilTableExists,
} from "@aws-sdk/client-dynamodb";
import {
  BatchGetCommand,
  type BatchGetCommandOutput,
  BatchWriteCommand,
  type BatchWriteCommandOutput,
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
  ScanCommand,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import * as zod from "zod";
import { findFieldMetaItems, getMetaItem } from "zod-meta";
import DatabaseAdaptor, {
  type Conflict,
  type DatabaseAdaptorOptions,
  groupUpsertRows,
  keyCondition,
  type PossiblySelectedResult,
} from "../../DatabaseAdaptor";
import {
  isZodTypeExtends,
  monotonicTimestamp,
  primaryKey,
  type Table,
  type TableColumnInfo,
  type TableIndex,
} from "../../index";
import {
  type FieldBinding,
  type InputOfTable,
  isFieldReference,
  type SelectCondition,
  type SelectFieldCondition,
  type SelectQuery,
  type SingleFieldBinding,
  type SqlResult,
  type StringKeys,
  type ValueOfTable,
  validateSelectCondition,
} from "../../QueryBuilder";
import { isRelationalQuery } from "../../RelationalQuery";
import { IncomingValue, SqlExpression } from "../../SqlExpression";
import type { Statement } from "../../Statement";

/**
 * A zodbase table on DynamoDB.
 *
 * - The field marked `primaryKey()` is the table's partition key.
 * - `table.addIndex(id, [partition, sort?])` is a global secondary index (projection ALL).
 * - A select is answered by the cheapest call that can: GetItem or BatchGetItem on the primary
 *   key, a Query on an index whose partition key the condition pins, and a Scan only when
 *   `allowScans` is set. Conditions the key cannot answer filter the items read.
 * - DynamoDB is schemaless, so `syncTable` creates the table and any missing indexes, and
 *   column changes are nothing to migrate.
 *
 * - An insert's `onConflict(key)` reads the row the key names first: `doNothing()` keeps it and
 *   `doUpdate(values?)` updates it, where `incoming(field)` is the inserted row's value.
 *
 * Refused, with a `ZodbaseDynamoDbError`: raw SQL (`execute`), joins, includes, projections,
 * subquery inserts, unique or partial indexes, computed values (SQL expressions), batches and
 * interactive transactions.
 */
export type DynamoDbAdaptorOptions = DatabaseAdaptorOptions<DynamoDBClient> & {
  /** Prepended to every table name, e.g. `"lovenote-main-"`. */
  tablePrefix?: string;
  /** Allow full-table scans for conditions no key or index answers. Off by default. */
  allowScans?: boolean;
};

export type ZodbaseDynamoDbErrorCode =
  | "dynamodb/unsupported"
  | "dynamodb/needs-index"
  | "dynamodb/duplicate-key"
  | "dynamodb/invalid-key";

export class ZodbaseDynamoDbError extends Error {
  constructor(
    readonly code: ZodbaseDynamoDbErrorCode,
    message: string,
  ) {
    super(`[zodbase] ${message}`);
    this.name = "ZodbaseDynamoDbError";
  }
}

const unsupported = (what: string) =>
  new ZodbaseDynamoDbError("dynamodb/unsupported", `${what} is not supported by the DynamoDB adaptor`);

type Item = Record<string, unknown>;
type KeyCondition = SelectFieldCondition & { operator: "=" | "<" | ">" | "<=" | ">=" };

const RANGE_OPERATORS = new Set(["=", "<", ">", "<=", ">="]);
const TRANSACT_LIMIT = 100;
const BATCH_GET_LIMIT = 100;
const BATCH_WRITE_LIMIT = 25;

const isCompound = (
  condition: SelectCondition,
): condition is SelectCondition & {
  type: "AND" | "OR";
  conditions: SelectCondition[];
} => "conditions" in condition;

/** The AND-ed field conditions at the top of a where clause; an OR stays whole. */
const topLevelConditions = (where: SelectCondition | undefined): SelectCondition[] => {
  if (!where) return [];
  const condition = validateSelectCondition(where);
  if (isCompound(condition) && condition.type === "AND")
    return condition.conditions.filter(Boolean).flatMap((child) => topLevelConditions(child));
  return [condition];
};

const likeToRegExp = (pattern: string) =>
  new RegExp(
    `^${pattern
      .split("")
      .map((char) => (char === "%" ? ".*" : char === "_" ? "." : char.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")))
      .join("")}$`,
    "s",
  );

const comparable = (value: unknown) => (value instanceof Date ? value.getTime() : value);

const deepEqual = (a: unknown, b: unknown): boolean => {
  if (a === b) return true;
  if (a instanceof Date || b instanceof Date) return comparable(a) === comparable(b);
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  return [...keys].every((key) => deepEqual((a as Item)[key], (b as Item)[key]));
};

/** Evaluates a condition against a decoded row, with SQL's NULL semantics. */
const matches = (row: Item, condition: SelectCondition): boolean => {
  if (isCompound(condition)) {
    const children = condition.conditions.filter(Boolean);
    return condition.type === "AND" ? children.every((c) => matches(row, c)) : children.some((c) => matches(row, c));
  }
  const field = condition as SelectFieldCondition;
  const actual = comparable(row[String(field.field.key)]);
  const raw = isFieldReference(field.value) ? row[String(field.value.key)] : field.value;
  const expected = comparable(raw);
  switch (field.operator) {
    case "=":
      return expected === null || expected === undefined
        ? actual == null
        : actual != null && deepEqual(actual, expected);
    case "!=":
      return expected === null || expected === undefined
        ? actual != null
        : actual != null && !deepEqual(actual, expected);
    case "<":
      return actual != null && expected != null && (actual as any) < (expected as any);
    case ">":
      return actual != null && expected != null && (actual as any) > (expected as any);
    case "<=":
      return actual != null && expected != null && (actual as any) <= (expected as any);
    case ">=":
      return actual != null && expected != null && (actual as any) >= (expected as any);
    case "IN":
      return actual != null && (raw as unknown[]).some((value) => deepEqual(actual, comparable(value)));
    case "NOT IN":
      return actual != null && !(raw as unknown[]).some((value) => deepEqual(actual, comparable(value)));
    case "LIKE":
      return typeof actual === "string" && likeToRegExp(String(raw)).test(actual);
    case "JSON_CONTAINS":
      return Array.isArray(actual) && actual.some((value) => deepEqual(value, raw));
    default:
      throw unsupported(`The ${String(field.operator)} operator`);
  }
};

export default class DynamoDbAdaptor extends DatabaseAdaptor<DynamoDBClient> {
  private documentClient?: { driver: DynamoDBClient; client: DynamoDBDocumentClient };

  constructor(protected override readonly options: DynamoDbAdaptorOptions) {
    super(options);
  }

  /** The document client over the driver, made once it is open (a lazy driver opens on first use). */
  private get documents(): DynamoDBDocumentClient {
    const driver = this.driver;
    if (this.documentClient?.driver !== driver)
      this.documentClient = {
        driver,
        client: DynamoDBDocumentClient.from(driver, {
          marshallOptions: { removeUndefinedValues: true, convertClassInstanceToMap: false },
        }),
      };
    return this.documentClient.client;
  }

  // ---- naming and keys

  tableName(table: Table) {
    return `${this.options.tablePrefix ?? ""}${String(table.id)}`;
  }

  private partitionKeyOf(table: Table): SingleFieldBinding {
    const keys = Object.values(table.fields).filter((field) => getMetaItem(field.schema, primaryKey));
    if (keys.length !== 1)
      throw new ZodbaseDynamoDbError(
        "dynamodb/invalid-key",
        `'${String(table.id)}' needs exactly one primaryKey() field to be its DynamoDB partition key`,
      );
    return keys[0] as SingleFieldBinding;
  }

  private indexKeys(index: TableIndex): { partition: SingleFieldBinding; sort?: SingleFieldBinding } {
    if (index.unique)
      throw new ZodbaseDynamoDbError(
        "dynamodb/unsupported",
        `Index '${index.id}' is unique; DynamoDB secondary indexes cannot enforce uniqueness`,
      );
    if (index.where) throw unsupported(`Partial index '${index.id}'`);
    if (index.fields.length === 0 || index.fields.length > 2)
      throw new ZodbaseDynamoDbError(
        "dynamodb/invalid-key",
        `Index '${index.id}' needs one partition field and an optional sort field`,
      );
    return { partition: index.fields[0]!, sort: index.fields[1] };
  }

  private attributeType(field: SingleFieldBinding): "S" | "N" {
    if (isZodTypeExtends(field.schema, zod.ZodNumber)) return "N";
    if (isZodTypeExtends(field.schema, zod.ZodString)) return "S";
    if (isZodTypeExtends(field.schema, zod.ZodDate)) return "S";
    throw new ZodbaseDynamoDbError(
      "dynamodb/invalid-key",
      `'${String(field.key)}' is a key, so it must be a string, number or date`,
    );
  }

  /** Attributes used as an index key: DynamoDB refuses NULL there, so a missing value is left out (a sparse index). */
  private indexedKeys(table: Table) {
    return new Set(table.indexes.flatMap((index) => index.fields.map((field) => String(field.key))));
  }

  // ---- values

  private encodeValue(value: unknown): unknown {
    if (value instanceof Date) return value.toISOString();
    if (typeof value === "bigint") return value.toString();
    if (Array.isArray(value)) return value.map((entry) => this.encodeValue(entry));
    if (value && typeof value === "object")
      return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, this.encodeValue(entry)]));
    return value;
  }

  private toItem(table: Table, values: Item): Item {
    const indexed = this.indexedKeys(table);
    const item: Item = {};
    for (const [key, value] of Object.entries(values)) {
      if (value === undefined) continue;
      if (value === null && indexed.has(key)) continue;
      item[key] = this.encodeValue(value);
    }
    return item;
  }

  private fromItem(table: Table, item: Item, fields?: FieldBinding<any>[]): Item {
    const keys =
      fields && !fields.some((field) => field.key === "*")
        ? fields.map((field) => String(field.key))
        : Object.keys(table.fields);
    // SQL returns every selected column; a missing attribute reads as null.
    return Object.fromEntries(
      keys.map((key) => [key, this.decodeSelectedValue(table.fields[key]?.schema, item[key] ?? null)]),
    );
  }

  private keyOf(table: Table, item: Item) {
    const partition = String(this.partitionKeyOf(table).key);
    return { [partition]: item[partition] };
  }

  // ---- reading

  /** Reads the items a where clause names, by the cheapest call that can, then filters them. */
  private async readItems(
    table: Table,
    where: SelectCondition | undefined,
    order?: { field: string; direction: "ASC" | "DESC" },
  ): Promise<{ items: Item[]; ordered: boolean }> {
    const conditions = topLevelConditions(where);
    const partition = String(this.partitionKeyOf(table).key);
    const filter = (items: Item[]) =>
      where ? items.filter((item) => matches(this.fromItem(table, item), where)) : items;

    const fieldCondition = (key: string, operators: Set<string>) =>
      conditions.find(
        (condition): condition is SelectFieldCondition =>
          !isCompound(condition) &&
          String((condition as SelectFieldCondition).field.key) === key &&
          operators.has((condition as SelectFieldCondition).operator) &&
          !isFieldReference((condition as SelectFieldCondition).value) &&
          (condition as SelectFieldCondition).value !== null,
      );

    // 1. The primary key, exactly: GetItem (strongly consistent).
    const byKey = fieldCondition(partition, new Set(["="]));
    if (byKey) {
      const { Item: item } = await this.documents.send(
        new GetCommand({
          TableName: this.tableName(table),
          Key: { [partition]: this.encodeValue(byKey.value) },
          ConsistentRead: true,
        }),
      );
      return { items: filter(item ? [item] : []), ordered: true };
    }

    // 2. A list of primary keys: BatchGetItem.
    const byKeys = fieldCondition(partition, new Set(["IN"]));
    if (byKeys) {
      const keys = [...new Set((byKeys.value as unknown[]).map((value) => this.encodeValue(value)))];
      const items: Item[] = [];
      for (let start = 0; start < keys.length; start += BATCH_GET_LIMIT) {
        let request: Record<string, any> | undefined = {
          [this.tableName(table)]: {
            Keys: keys.slice(start, start + BATCH_GET_LIMIT).map((key) => ({ [partition]: key })),
            ConsistentRead: true,
          },
        };
        while (request && Object.keys(request).length > 0) {
          const response: BatchGetCommandOutput = await this.documents.send(
            new BatchGetCommand({ RequestItems: request }),
          );
          items.push(...((response.Responses?.[this.tableName(table)] as Item[]) ?? []));
          request = response.UnprocessedKeys;
        }
      }
      return { items: filter(items), ordered: false };
    }

    // 3. An index whose partition key the condition pins: Query, sorted by its sort key.
    const candidates = table.indexes
      .map((index) => ({ index, ...this.indexKeys(index) }))
      .map((candidate) => ({
        ...candidate,
        hash: fieldCondition(String(candidate.partition.key), new Set(["="])),
        range: candidate.sort ? fieldCondition(String(candidate.sort.key), RANGE_OPERATORS) : undefined,
      }))
      .filter((candidate) => candidate.hash)
      // Prefer an index that also sorts the way asked, then one that narrows by its sort key.
      .sort(
        (a, b) =>
          Number(String(b.sort?.key) === order?.field) - Number(String(a.sort?.key) === order?.field) ||
          Number(Boolean(b.range)) - Number(Boolean(a.range)),
      );
    const best = candidates[0];
    if (best) {
      const names: Record<string, string> = { "#p": String(best.partition.key) };
      const values: Record<string, unknown> = { ":p": this.encodeValue(best.hash!.value) };
      let keyCondition = "#p = :p";
      if (best.range && best.sort) {
        names["#s"] = String(best.sort.key);
        values[":s"] = this.encodeValue(best.range.value);
        keyCondition += ` AND #s ${(best.range as KeyCondition).operator} :s`;
      }
      const sortedByIndex = Boolean(best.sort) && String(best.sort!.key) === order?.field;
      const input: QueryCommandInput = {
        TableName: this.tableName(table),
        IndexName: best.index.id,
        KeyConditionExpression: keyCondition,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        ScanIndexForward: sortedByIndex ? order!.direction === "ASC" : true,
      };
      const items: Item[] = [];
      let startKey: Record<string, unknown> | undefined;
      do {
        const response = await this.documents.send(new QueryCommand({ ...input, ExclusiveStartKey: startKey }));
        items.push(...((response.Items as Item[]) ?? []));
        startKey = response.LastEvaluatedKey;
      } while (startKey);
      return { items: filter(items), ordered: sortedByIndex };
    }

    // 4. Nothing answers it: a Scan, only when allowed.
    if (!this.options.allowScans) {
      const fields = conditions
        .filter((condition) => !isCompound(condition))
        .map((condition) => String((condition as SelectFieldCondition).field.key));
      throw new ZodbaseDynamoDbError(
        "dynamodb/needs-index",
        `Reading '${String(table.id)}'${fields.length ? ` by ${fields.join(", ")}` : ""} would scan the whole table: add an index (table.addIndex) whose first field the condition pins with equals(), or set allowScans`,
      );
    }
    const items: Item[] = [];
    let startKey: Record<string, unknown> | undefined;
    do {
      const response = await this.documents.send(
        new ScanCommand({ TableName: this.tableName(table), ExclusiveStartKey: startKey, ConsistentRead: true }),
      );
      items.push(...((response.Items as Item[]) ?? []));
      startKey = response.LastEvaluatedKey;
    } while (startKey);
    return { items: filter(items), ordered: false };
  }

  private refuseRelational(query: SelectQuery) {
    if (query.joins?.length) throw unsupported("A join");
    if (query.includes && Object.keys(query.includes).length) throw unsupported("include()");
    if (query.projection || isRelationalQuery(query)) throw unsupported("A projection");
  }

  async executeSelect<TTable extends Table, TLimit extends number>(
    query: SelectQuery<Table, TLimit>,
  ): Promise<SqlResult<ValueOfTable<TTable>, TLimit>> {
    this.refuseRelational(query);
    if (query.orderBy.length > 1) throw unsupported("Ordering by more than one field");
    const order = query.orderBy[0]
      ? { field: String(query.orderBy[0].field.key), direction: query.orderBy[0].direction }
      : undefined;
    const { items, ordered } = await this.readItems(query.table, query.where, order);
    let rows = items.map((item) => this.fromItem(query.table, item));
    if (order && !ordered) {
      const factor = order.direction === "DESC" ? -1 : 1;
      rows = rows.sort((a, b) => {
        const left = comparable(a[order.field]) as any;
        const right = comparable(b[order.field]) as any;
        // NULL sorts first ascending (and last descending), as SQLite and MySQL order it.
        if (left == null && right == null) return 0;
        if (left == null) return -factor;
        if (right == null) return factor;
        return (left < right ? -1 : left > right ? 1 : 0) * factor;
      });
    }
    const start = query.offset ?? 0;
    const sliced = rows.slice(start, query.limit !== undefined ? start + query.limit : undefined);
    const selected = sliced.map((row) =>
      query.fields.some((field) => field.key === "*")
        ? row
        : Object.fromEntries(query.fields.map((field) => [String(field.key), row[String(field.key)]])),
    );
    return { results: selected, first: selected[0] } as any;
  }

  async executeQueryCount(query: SelectQuery): Promise<SqlResult> {
    this.refuseRelational({ ...query, projection: undefined });
    const { items } = await this.readItems(query.table, query.where);
    const rows = items.map((item) => this.fromItem(query.table, item));
    return this.countRows(rows, query.fields);
  }

  /** Reads the matching items and counts them, up to `max`. */
  override async executeCappedCount(query: SelectQuery, max: number): Promise<number> {
    if (!Number.isSafeInteger(max) || max < 0) throw new Error("Invalid count limit");
    this.refuseRelational({ ...query, projection: undefined });
    const { items } = await this.readItems(query.table, query.where);
    return Math.min(items.length, max);
  }

  async executeCount<TTable extends Table, TKey extends StringKeys<ValueOfTable<TTable>>>(
    table: TTable,
    fields: SingleFieldBinding<ValueOfTable<TTable>, TKey>[],
    where: SelectCondition<ValueOfTable<TTable>> | undefined,
  ): Promise<SqlResult<Record<TKey, number>, 1>> {
    const { items } = await this.readItems(table, where);
    return this.countRows(
      items.map((item) => this.fromItem(table, item)),
      fields,
    ) as any;
  }

  private countRows(rows: Item[], fields: FieldBinding<any>[]): SqlResult {
    const row = Object.fromEntries(
      fields.map((field) =>
        field.key === "*"
          ? ["_count", rows.length]
          : [String(field.key), rows.filter((entry) => entry[String(field.key)] != null).length],
      ),
    );
    return { results: [row], first: row };
  }

  // ---- writing

  private translateWriteError(error: unknown, table: Table): never {
    const name = (error as { name?: string }).name;
    const reasons = (error as { CancellationReasons?: { Code?: string }[] }).CancellationReasons;
    if (
      name === "ConditionalCheckFailedException" ||
      (name === "TransactionCanceledException" && reasons?.some((reason) => reason.Code === "ConditionalCheckFailed"))
    )
      throw new ZodbaseDynamoDbError(
        "dynamodb/duplicate-key",
        `A row with that ${String(this.partitionKeyOf(table).key)} already exists in '${String(table.id)}'`,
      );
    throw error;
  }

  async executeInsert<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>,
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>, 1>> {
    const partition = String(this.partitionKeyOf(table).key);
    const item = this.toItem(table, values as Item);
    try {
      await this.documents.send(
        new PutCommand({
          TableName: this.tableName(table),
          Item: item,
          ConditionExpression: "attribute_not_exists(#k)",
          ExpressionAttributeNames: { "#k": partition },
        }),
      );
    } catch (error) {
      this.translateWriteError(error, table);
    }
    if (!shouldReturn) return { results: [], first: undefined } as any;
    const row = this.fromItem(table, item);
    return { results: [row], first: row } as any;
  }

  /** Up to 100 rows go in one transaction, all or none, as a multi-row SQL insert does. */
  async executeInsertMany<TTable extends Table>(
    table: TTable,
    values: InputOfTable<TTable>[],
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>, number>> {
    const partition = String(this.partitionKeyOf(table).key);
    const items = values.map((value) => this.toItem(table, value as Item));
    for (let start = 0; start < items.length; start += TRANSACT_LIMIT) {
      try {
        await this.documents.send(
          new TransactWriteCommand({
            TransactItems: items.slice(start, start + TRANSACT_LIMIT).map((item) => ({
              Put: {
                TableName: this.tableName(table),
                Item: item,
                ConditionExpression: "attribute_not_exists(#k)",
                ExpressionAttributeNames: { "#k": partition },
              },
            })),
          }),
        );
      } catch (error) {
        this.translateWriteError(error, table);
      }
    }
    if (!shouldReturn) return { results: [], first: undefined } as any;
    const rows = items.map((item) => this.fromItem(table, item));
    return { results: rows, first: rows[0] } as any;
  }

  /** Updates one item by its key. A value computed from the column (monotonicTimestamp) is read first. */
  private async updateItem(table: Table, item: Item, values: Item): Promise<Item> {
    const indexed = this.indexedKeys(table);
    const partition = String(this.partitionKeyOf(table).key);
    const monotonic = new Set(findFieldMetaItems(table.schema, monotonicTimestamp).map((field) => String(field.key)));
    const names: Record<string, string> = {};
    const attributeValues: Record<string, unknown> = {};
    const sets: string[] = [];
    const removes: string[] = [];
    const conditions = ["attribute_exists(#k)"];
    names["#k"] = partition;
    let n = 0;
    for (const [key, raw] of Object.entries(values)) {
      if (raw === undefined || key === partition) continue;
      const name = `#f${n}`;
      names[name] = key;
      let value = raw;
      if (isFieldReference(value) || (value instanceof SqlExpression && !monotonic.has(key)))
        throw unsupported("A computed value (an SQL expression)");
      if (value instanceof SqlExpression) {
        // monotonicTimestamp: one more than the stored value if that is now or later, else now.
        const now = Date.now();
        const current = item[key];
        value = typeof current === "number" && current >= now ? current + 1 : now;
        // Applied only if nobody changed it since it was read; the caller reads again and retries.
        if (current === undefined) conditions.push(`attribute_not_exists(${name})`);
        else {
          attributeValues[`:was${n}`] = current;
          conditions.push(`${name} = :was${n}`);
        }
      }
      if (value === null && indexed.has(key)) {
        removes.push(name);
      } else {
        attributeValues[`:v${n}`] = this.encodeValue(value);
        sets.push(`${name} = :v${n}`);
      }
      n++;
    }
    if (sets.length === 0 && removes.length === 0) return item;
    const response = await this.documents.send(
      new UpdateCommand({
        TableName: this.tableName(table),
        Key: this.keyOf(table, item),
        UpdateExpression: [
          sets.length ? `SET ${sets.join(", ")}` : "",
          removes.length ? `REMOVE ${removes.join(", ")}` : "",
        ]
          .filter(Boolean)
          .join(" "),
        ConditionExpression: conditions.join(" AND "),
        ExpressionAttributeNames: names,
        ...(Object.keys(attributeValues).length ? { ExpressionAttributeValues: attributeValues } : {}),
        ReturnValues: "ALL_NEW",
      }),
    );
    return response.Attributes as Item;
  }

  async executeUpdate<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    where: SelectCondition<ValueOfTable<TTable>>,
    shouldReturn = false,
  ): Promise<PossiblySelectedResult<ValueOfTable<TTable>>> {
    const { items } = await this.readItems(table, where);
    const updated: Item[] = [];
    for (const item of items) {
      // A row another writer changed or removed since it was read is read again, a few times.
      let current: Item | undefined = item;
      for (let attempt = 0; current; attempt++) {
        try {
          updated.push(await this.updateItem(table, current, values as Item));
          break;
        } catch (error) {
          if ((error as { name?: string }).name !== "ConditionalCheckFailedException" || attempt >= 3) throw error;
          const { Item: fresh }: { Item?: Record<string, any> } = await this.documents.send(
            new GetCommand({ TableName: this.tableName(table), Key: this.keyOf(table, current), ConsistentRead: true }),
          );
          current = fresh && matches(this.fromItem(table, fresh), where) ? (fresh as Item) : undefined;
        }
      }
    }
    const rows = shouldReturn ? updated.map((item) => this.fromItem(table, item)) : [];
    return { results: rows, first: rows[0], selected: shouldReturn } as any;
  }

  /** The condition naming the row `values` conflicts with on the conflict's key. */
  private conflictCondition(conflict: Conflict, values: Item): SelectCondition {
    if (conflict.where) throw unsupported("A conflict target with a condition (a partial index)");
    return keyCondition(conflict.fields, values);
  }

  /** What a conflict's update sets on the row it conflicts with, given the inserted row's values. */
  private conflictChanges(conflict: Conflict, values: Item): Item {
    if (!conflict.set) {
      const keys = new Set(conflict.fields.map((field) => String(field.key)));
      return Object.fromEntries(Object.entries(values).filter(([key]) => !keys.has(key)));
    }
    return Object.fromEntries(
      Object.entries(conflict.set)
        .filter(([, value]) => value !== undefined)
        .map(([key, value]) => {
          if (value instanceof IncomingValue) return [key, values[String(value.field.key)]];
          if (value instanceof SqlExpression) throw unsupported("A computed value (an SQL expression)");
          return [key, value];
        }),
    );
  }

  /** Inserts the row, or updates the one its key names, as the conflict says. */
  async executeUpsert<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>,
    conflict: Conflict,
  ): Promise<SqlResult<void, 0>> {
    const row = values as Item;
    const { items } = await this.readItems(table, this.conflictCondition(conflict, row));
    if (items.length === 0) await this.executeInsert(table, values as any);
    else if (conflict.action === "update") {
      const changes = this.conflictChanges(conflict, row);
      for (const item of items) if (Object.keys(changes).length) await this.updateItem(table, item, changes);
    }
    return { results: [], first: undefined };
  }

  override async executeUpsertMany<TTable extends Table>(
    table: TTable,
    values: Partial<InputOfTable<TTable>>[],
    conflict: Conflict,
  ): Promise<SqlResult<void, 0>> {
    const keys = conflict.fields.map((field) => String(field.key));
    for (const rows of groupUpsertRows(values as Item[], keys))
      for (const row of rows) await this.executeUpsert(table, row as any, conflict);
    return { results: [], first: undefined };
  }

  /** Inserts the rows whose key names no row yet, returning those it inserted when asked. */
  override async executeInsertIgnoring<TTable extends Table>(
    table: TTable,
    rows: InputOfTable<TTable>[],
    conflict: Conflict,
    shouldReturn = false,
  ): Promise<SqlResult<ValueOfTable<TTable>>> {
    const inserted: Item[] = [];
    for (const row of rows) {
      const { items } = await this.readItems(table, this.conflictCondition(conflict, row as Item));
      if (items.length) continue;
      try {
        const result = await this.executeInsert(table, row, true);
        inserted.push(result.first as Item);
      } catch (error) {
        // Another writer inserted the same key since it was read: that row is kept.
        if (!(error instanceof ZodbaseDynamoDbError && error.code === "dynamodb/duplicate-key")) throw error;
      }
    }
    return shouldReturn ? ({ results: inserted, first: inserted[0] } as any) : { results: [], first: undefined };
  }

  /** Updates each row by its key (and `where`, with `incoming(field)` its own values). */
  async executeUpdateMany<
    TTable extends Table,
    TValue extends Partial<InputOfTable<TTable>> & zod.ZodRawShape,
    TKey extends StringKeys<ValueOfTable<TTable>>,
  >(
    table: TTable,
    values: TValue[],
    fields: readonly SingleFieldBinding<TValue, TKey>[],
    where?: SelectCondition,
  ): Promise<SqlResult<void, 0>> {
    const keys = new Set(fields.map((field) => String(field.key)));
    for (const value of values) {
      const row = value as Item;
      const changes = Object.fromEntries(Object.entries(row).filter(([key]) => !keys.has(key)));
      await this.executeUpdate(table, changes as any, keyCondition(fields as any, row, where) as any);
    }
    return { results: [], first: undefined };
  }

  async executeDelete<TTable extends Table>(
    table: TTable,
    where: SelectCondition<ValueOfTable<TTable>>,
  ): Promise<SqlResult<void, 0>> {
    const { items } = await this.readItems(table, where);
    const keys = items.map((item) => this.keyOf(table, item));
    for (let start = 0; start < keys.length; start += BATCH_WRITE_LIMIT) {
      let request: Record<string, any> | undefined = {
        [this.tableName(table)]: keys
          .slice(start, start + BATCH_WRITE_LIMIT)
          .map((key) => ({ DeleteRequest: { Key: key } })),
      };
      while (request && Object.keys(request).length > 0) {
        const response: BatchWriteCommandOutput = await this.documents.send(
          new BatchWriteCommand({ RequestItems: request }),
        );
        request = response.UnprocessedItems;
      }
    }
    return { results: [], first: undefined };
  }

  /** One row, by its key: a single DeleteItem. */
  async deleteByKey(table: Table, key: unknown) {
    await this.documents.send(
      new DeleteCommand({
        TableName: this.tableName(table),
        Key: { [String(this.partitionKeyOf(table).key)]: this.encodeValue(key) },
      }),
    );
  }

  // ---- schema

  override async createTable(table: Table, name?: string): Promise<SqlResult> {
    const tableName = name ?? this.tableName(table);
    const partition = this.partitionKeyOf(table);
    const attributes = new Map<string, AttributeDefinition>();
    const define = (field: SingleFieldBinding) =>
      attributes.set(String(field.key), {
        AttributeName: String(field.key),
        AttributeType: this.attributeType(field),
      });
    define(partition);
    const indexes: GlobalSecondaryIndex[] = table.indexes.map((index) => {
      const { partition: hash, sort } = this.indexKeys(index);
      define(hash);
      if (sort) define(sort);
      return {
        IndexName: index.id,
        KeySchema: [
          { AttributeName: String(hash.key), KeyType: "HASH" },
          ...(sort ? [{ AttributeName: String(sort.key), KeyType: "RANGE" as const }] : []),
        ],
        Projection: { ProjectionType: "ALL" },
      };
    });
    if (await this.describe(tableName)) return { results: [], first: undefined };
    await this.driver.send(
      new CreateTableCommand({
        TableName: tableName,
        BillingMode: "PAY_PER_REQUEST",
        AttributeDefinitions: [...attributes.values()],
        KeySchema: [{ AttributeName: String(partition.key), KeyType: "HASH" }],
        ...(indexes.length ? { GlobalSecondaryIndexes: indexes } : {}),
      }),
    );
    await waitUntilTableExists({ client: this.driver, maxWaitTime: 120 }, { TableName: tableName });
    return { results: [], first: undefined };
  }

  private async describe(tableName: string) {
    try {
      return (await this.driver.send(new DescribeTableCommand({ TableName: tableName }))).Table;
    } catch (error) {
      if (
        error instanceof ResourceNotFoundException ||
        (error as { name?: string }).name === "ResourceNotFoundException"
      )
        return undefined;
      throw error;
    }
  }

  /**
   * Adds the indexes the table declares and DynamoDB lacks, one at a time as DynamoDB requires,
   * returning their names. An index whose keys changed is not rebuilt: DynamoDB can't alter one.
   */
  override async syncTableIndexes(table: Table): Promise<string[]> {
    const tableName = this.tableName(table);
    const created: string[] = [];
    for (const index of table.indexes) {
      const existing = (await this.describe(tableName))?.GlobalSecondaryIndexes ?? [];
      if (existing.some((gsi) => gsi.IndexName === index.id)) continue;
      const { partition, sort } = this.indexKeys(index);
      await this.driver.send(
        new UpdateTableCommand({
          TableName: tableName,
          AttributeDefinitions: [partition, ...(sort ? [sort] : [])].map((field) => ({
            AttributeName: String(field.key),
            AttributeType: this.attributeType(field),
          })),
          GlobalSecondaryIndexUpdates: [
            {
              Create: {
                IndexName: index.id,
                KeySchema: [
                  { AttributeName: String(partition.key), KeyType: "HASH" },
                  ...(sort ? [{ AttributeName: String(sort.key), KeyType: "RANGE" as const }] : []),
                ],
                Projection: { ProjectionType: "ALL" },
              },
            },
          ],
        }),
      );
      for (let wait = 0; wait < 120; wait++) {
        const status = (await this.describe(tableName))?.GlobalSecondaryIndexes?.find(
          (gsi) => gsi.IndexName === index.id,
        )?.IndexStatus;
        if (status === "ACTIVE") break;
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
      created.push(index.id);
    }
    return created;
  }

  /** DynamoDB keeps no columns: there is nothing to diff or migrate. */
  async fetchTableColumns(_table: Table): Promise<SqlResult<TableColumnInfo>> {
    return { results: [], first: undefined };
  }

  async processDiff(): Promise<void> {}

  createIndexStatement(): Statement {
    throw unsupported("SQL generation");
  }

  // ---- what DynamoDB has no counterpart for

  async execute(_statement: Statement): Promise<SqlResult> {
    throw unsupported("Raw SQL");
  }

  buildJsonArrayContainsSql(): Statement {
    throw unsupported("SQL generation");
  }

  override async executeInsertSubqueries(): Promise<SqlResult> {
    throw unsupported("An insert with subqueries");
  }

  override transaction<TResult>(): Promise<TResult> {
    return Promise.reject(
      unsupported("An interactive transaction (DynamoDB transactions are a single request; use insertMany)"),
    );
  }
}
