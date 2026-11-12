import { parse } from "@rsql/parser";
import * as zod from "zod";
import type { SelectCondition, SingleFieldBinding, ValueOfTable } from "../QueryBuilder";
import type { Table } from "../Table";
import { isZodTypeExtends } from "../ZodUtils";

const OPERATOR_MAP = {
  "==": "equals",
  "!=": "notEquals",
  ">": "greaterThan",
  ">=": "greaterThanOrEquals",
  "<": "lessThan",
  "<=": "lessThanOrEquals",
  "=in=": "in",
  "=out=": "notIn",
  "=like=": "like",
} as const;

// The package targets a TypeScript lib older than `Object.hasOwn`.
const hasOwn = (object: object, key: string) => Object.getOwnPropertyDescriptor(object, key) !== undefined;

/** An RSQL comparison operator the converter implements. */
export type RsqlOperator = keyof typeof OPERATOR_MAP;

export interface RsqlOptions<TTable extends Table> {
  /**
   * The selectors a filter may name, each with the table field it reads. A selector not listed
   * throws, so a filter can't reach columns the application doesn't expose. Without it, a filter
   * may name any field of the table.
   */
  fields?: Readonly<Record<string, keyof ValueOfTable<TTable> & string>>;
  /** The operators a filter may use. Without it, every supported operator is allowed. */
  operators?: readonly RsqlOperator[];
  /**
   * The operators particular selectors may use instead of `operators`, such as only `=in=` and
   * `=out=` for a JSON array field, whose other comparisons would read the stored JSON text.
   */
  fieldOperators?: Readonly<Record<string, readonly RsqlOperator[]>>;
  /** Maps a selector’s API values to their stored representation. */
  fieldValues?: Readonly<Record<string, (value: string) => string | number | boolean | null>>;
}

/** A filter that can't be converted. `field` names the selector when it isn't a filterable field. */
export class RsqlFilterError extends Error {
  readonly field: string | undefined;

  constructor(message: string, field?: string) {
    super(message);
    this.name = "RsqlFilterError";
    this.field = field;
  }
}

const parseValue = (schema: zod.ZodType, value: string): string | number | boolean | null => {
  const arrayType = isZodTypeExtends(schema, zod.ZodArray);
  const valueSchema: zod.ZodType = arrayType ? ((arrayType as zod.ZodArray).element as unknown as zod.ZodType) : schema;

  if (
    value === "null" &&
    (isZodTypeExtends(valueSchema, zod.ZodNull) || isZodTypeExtends(valueSchema, zod.ZodNullable))
  ) {
    return null;
  }
  if (isZodTypeExtends(valueSchema, zod.ZodBoolean)) {
    if (value === "true") return true;
    if (value === "false") return false;
    throw new Error(`Invalid boolean value: ${value}`);
  }
  if (isZodTypeExtends(valueSchema, zod.ZodNumber)) {
    const numberValue = Number(value);
    if (Number.isFinite(numberValue) && value !== "") {
      return numberValue;
    }
    throw new Error(`Invalid numeric value: ${value}`);
  }

  return value;
};

const astNodeToCondition = <TTable extends Table>(
  table: TTable,
  node: any,
  options: RsqlOptions<TTable>,
): SelectCondition<ValueOfTable<TTable>> | undefined => {
  // Handle logical operators (AND/OR)
  if (node.type === "LOGIC") {
    const left = node.left ? astNodeToCondition(table, node.left, options) : undefined;
    const right = node.right ? astNodeToCondition(table, node.right, options) : undefined;

    if (!left || !right) {
      return left || right;
    }

    // RSQL uses ";" for AND and "," for OR
    if (node.operator === "and" || node.operator === ";") {
      return left.and(right);
    }
    if (node.operator === "or" || node.operator === ",") {
      return left.or(right);
    }

    throw new Error(`Unsupported logic operator: ${node.operator}`);
  }

  // Handle comparison operators
  if (node.type === "COMPARISON") {
    // RSQL parser uses 'left' property for the field selector (SelectorNode)
    const leftNode = (node as any).left;
    const selector = String(leftNode?.selector || leftNode);
    const fieldName = options.fields
      ? hasOwn(options.fields, selector)
        ? options.fields[selector]
        : undefined
      : selector;

    const fieldBinding =
      fieldName !== undefined && hasOwn(table.fields, fieldName)
        ? ((table as any)[`$${fieldName}`] as SingleFieldBinding<ValueOfTable<TTable>> | undefined)
        : undefined;

    if (!fieldBinding) {
      throw new RsqlFilterError(`Field "${selector}" not found in table`, selector);
    }

    const method = hasOwn(OPERATOR_MAP, node.operator) ? OPERATOR_MAP[node.operator as RsqlOperator] : undefined;

    const allowed =
      options.fieldOperators && hasOwn(options.fieldOperators, selector)
        ? options.fieldOperators[selector]
        : options.operators;
    if (!method || (allowed && !allowed.includes(node.operator))) {
      throw new RsqlFilterError(`Unsupported operator: ${node.operator}`);
    }

    // Parse values - RSQL parser uses 'right' property for the value(s)
    const rightNode = (node as any).right;
    // Extract the actual value from ComparisonNode or ValueNode
    const rightValue = rightNode?.arguments || rightNode?.value || rightNode;
    const readValue = options.fieldValues && hasOwn(options.fieldValues, selector)
      ? options.fieldValues[selector]!
      : (value: string) => parseValue(fieldBinding.schema, value);
    const values = (Array.isArray(rightValue) ? rightValue : [rightValue]).map(readValue);

    // Handle different operators
    // `*` in an `==` or `!=` string value is a wildcard for any run of characters.
    const pattern = typeof values[0] === "string" && values[0].includes("*") ? values[0] : undefined;

    switch (method) {
      case "equals":
        return pattern === undefined ? fieldBinding.equals(values[0] as any) : fieldBinding.matches(pattern);
      case "notEquals":
        return pattern === undefined ? fieldBinding.notEquals(values[0] as any) : fieldBinding.notMatches(pattern);
      case "greaterThan":
        return fieldBinding.greaterThan(values[0] as any);
      case "greaterThanOrEquals":
        return fieldBinding.greaterThanOrEquals(values[0] as any);
      case "lessThan":
        return fieldBinding.lessThan(values[0] as any);
      case "lessThanOrEquals":
        return fieldBinding.lessThanOrEquals(values[0] as any);
      // A JSON array matches `=in=` when it holds any of the values, and `=out=` when it holds none.
      case "in":
        if (isZodTypeExtends(fieldBinding.schema, zod.ZodArray)) {
          const [first, ...rest] = values.map((value) => fieldBinding.contains(value));
          return rest.length === 0 ? first : first!.or(...rest);
        }
        return fieldBinding.in(values as any);
      case "notIn":
        if (isZodTypeExtends(fieldBinding.schema, zod.ZodArray)) {
          const [first, ...rest] = values.map((value) => fieldBinding.notContains(value));
          return rest.length === 0 ? first : first!.and(...rest);
        }
        return fieldBinding.notIn(values as any);
      case "like":
        return fieldBinding.like(values[0] as any);
      default:
        throw new Error(`Unsupported method: ${method}`);
    }
  }

  return undefined;
};

export const rsqlToCondition = <TTable extends Table>(
  table: TTable,
  filterString: string | undefined,
  options: RsqlOptions<TTable> = {},
): SelectCondition<ValueOfTable<TTable>> | undefined => {
  if (!filterString) {
    return undefined;
  }

  try {
    const ast = parse(filterString);
    return astNodeToCondition(table, ast, options);
  } catch (error) {
    throw new RsqlFilterError(
      `Failed to parse RSQL filter: ${error instanceof Error ? error.message : String(error)}`,
      error instanceof RsqlFilterError ? error.field : undefined,
    );
  }
};
