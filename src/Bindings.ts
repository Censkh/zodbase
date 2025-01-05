import type { SelectCondition, SingleFieldBinding, ValueOfTable } from "./QueryBuilder";
import { TO_SQL_SYMBOL } from "./Statement";
import type { Bindings, Table } from "./Table";

export const createTableBinding = <TTable extends Table>(
  table: TTable,
): Bindings<ValueOfTable<TTable>> => {
  const binding: Bindings<ValueOfTable<TTable>> = {} as any;
  for (const field of Object.keys(table.schema.shape)) {
    const fieldBinding: SingleFieldBinding<any, any> = {
      key: Object.assign(field, {
        [TO_SQL_SYMBOL]: () => field,
      }),
      [TO_SQL_SYMBOL]: () => field,
      table: table as any,
      schema: table.schema.shape[field],

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
    binding[field] = fieldBinding;
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
