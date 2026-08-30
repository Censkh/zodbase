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
    if (!Number.isNaN(numberValue) && value !== "") {
      return numberValue;
    }
    throw new Error(`Invalid numeric value: ${value}`);
  }

  return value;
};

const astNodeToCondition = <TTable extends Table>(
  table: TTable,
  node: any,
): SelectCondition<ValueOfTable<TTable>> | undefined => {
  // Handle logical operators (AND/OR)
  if (node.type === "LOGIC") {
    const left = node.left ? astNodeToCondition(table, node.left) : undefined;
    const right = node.right ? astNodeToCondition(table, node.right) : undefined;

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
    const fieldName = leftNode?.selector || leftNode;

    const fieldBinding = (table as any)[`$${fieldName}`] as SingleFieldBinding<ValueOfTable<TTable>> | undefined;

    if (!fieldBinding) {
      throw new Error(`Field "${fieldName}" not found in table`);
    }

    const method = OPERATOR_MAP[node.operator as keyof typeof OPERATOR_MAP];

    if (!method) {
      throw new Error(`Unsupported operator: ${node.operator}`);
    }

    // Parse values - RSQL parser uses 'right' property for the value(s)
    const rightNode = (node as any).right;
    // Extract the actual value from ComparisonNode or ValueNode
    const rightValue = rightNode?.arguments || rightNode?.value || rightNode;
    const values = Array.isArray(rightValue)
      ? rightValue.map((arg: string) => parseValue(fieldBinding.schema, arg))
      : [parseValue(fieldBinding.schema, rightValue)];

    // Handle different operators
    switch (method) {
      case "equals":
        return fieldBinding.equals(values[0] as any);
      case "notEquals":
        return fieldBinding.notEquals(values[0] as any);
      case "greaterThan":
        return fieldBinding.greaterThan(values[0] as any);
      case "greaterThanOrEquals":
        return fieldBinding.greaterThanOrEquals(values[0] as any);
      case "lessThan":
        return fieldBinding.lessThan(values[0] as any);
      case "lessThanOrEquals":
        return fieldBinding.lessThanOrEquals(values[0] as any);
      case "in":
        if (isZodTypeExtends(fieldBinding.schema, zod.ZodArray)) {
          const conditions = values.map((value) => fieldBinding.contains(value));
          let combinedCondition: SelectCondition<ValueOfTable<TTable>> = conditions[0]!;
          for (const nextCondition of conditions.slice(1)) {
            combinedCondition = combinedCondition.or(nextCondition);
          }
          return combinedCondition;
        }
        return fieldBinding.in(values as any);
      case "notIn":
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
): SelectCondition<ValueOfTable<TTable>> | undefined => {
  if (!filterString) {
    return undefined;
  }

  try {
    const ast = parse(filterString);
    return astNodeToCondition(table, ast);
  } catch (error) {
    throw new Error(`Failed to parse RSQL filter: ${error instanceof Error ? error.message : String(error)}`);
  }
};
