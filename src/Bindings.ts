import { getZodTypeFields } from "zod-meta";
import type { SelectCondition, SingleFieldBinding, ValueOfTable } from "./QueryBuilder";
import { TO_SQL_SYMBOL } from "./Statement";
import type { Bindings, Table } from "./Table";

export const createTableBinding = <TTable extends Table>(
  table: TTable,
): Bindings<ValueOfTable<TTable>> => {
  const binding: Bindings<ValueOfTable<TTable>> = {} as any;

  const fields = getZodTypeFields(table.schema);

  for (const field of fields) {
    const fieldBinding: SingleFieldBinding<any, any> = {
      key: Object.assign(field.key, {
        [TO_SQL_SYMBOL]: () => field.key,
      }),
      [TO_SQL_SYMBOL]: () => field.key,
      table: table as any,
      schema: field.schema,

      equals(value) {
        return createSelectCondition({
          field: fieldBinding,
          operator: "=",
          value,
        }) as any;
      },

      like(value) {
        return createSelectCondition({
          field: fieldBinding,
          operator: "LIKE",
          value,
        }) as any;
      },

      greaterThan(value) {
        return createSelectCondition({
          field: fieldBinding,
          operator: ">",
          value,
        }) as any;
      },

      lessThan(value) {
        return createSelectCondition({
          field: fieldBinding,
          operator: "<",
          value,
        }) as any;
      },

      greaterThanOrEquals(value) {
        return createSelectCondition({
          field: fieldBinding,
          operator: ">=",
          value,
        }) as any;
      },

      lessThanOrEquals(value) {
        return createSelectCondition({
          field: fieldBinding,
          operator: "<=",
          value,
        }) as any;
      },

      notEquals(value) {
        return createSelectCondition({
          field: fieldBinding,
          operator: "!=",
          value,
        }) as any;
      },

      in(values) {
        return createSelectCondition({
          field: fieldBinding,
          operator: "IN",
          value: values,
        }) as any;
      },
    };
    // @ts-ignore
    binding[field.key] = fieldBinding;
  }

  return binding as Bindings<ValueOfTable<TTable>>;
};

const createSelectCondition = (options: Omit<SelectCondition, "and" | "or">): SelectCondition => {
  return {
    ...options,

    and(...condition: SelectCondition[]): SelectCondition {
      return createSelectCondition({
        type: "AND",
        conditions: [this, ...condition],
      });
    },

    or(...condition: SelectCondition[]): SelectCondition {
      return createSelectCondition({
        type: "OR",
        conditions: [this, ...condition],
      });
    },
  } as SelectCondition;
};
