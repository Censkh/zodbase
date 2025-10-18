import { parse } from "@rsql/parser";
import type { SelectCondition, SingleFieldBinding, ValueOfTable } from "../QueryBuilder";
import type { Table } from "../Table";

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

const parseValue = (value: string): string | number | boolean | null => {
  if (value === "null") return null;
  // SQLite stores booleans as 1 and 0, so convert true/false to numbers
  if (value === "true") return 1;
  if (value === "false") return 0;

  const num = Number(value);
  if (!Number.isNaN(num) && value !== "") {
    return num;
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
      ? rightValue.map((arg: string) => parseValue(arg))
      : [parseValue(rightValue)];

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
        return fieldBinding.in(values as any);
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
