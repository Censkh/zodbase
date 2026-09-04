import * as zod from "zod";
import { createMetaType } from "zod-meta";
import type { SingleFieldBinding } from "./QueryBuilder";
import { isZodTypeExtends } from "./ZodUtils";

export const foreignKey = createMetaType<{
  field: SingleFieldBinding;
  onDelete?: ForeignKeyAction;
}>({
  id: "foreignKey",
  check: (type) => {
    if (isZodTypeExtends(type, zod.ZodString) || isZodTypeExtends(type, zod.ZodNumber)) {
      // is foreign key field binding the same type as the field type
      /*if (options?.field?.schema !== type) {
        return {
          success: false,
          message: "Foreign key field type must be the same as the field type",
        };
      }*/

      return {
        success: true,
      };
    }
    return {
      success: false,
      message: "Foreign key must be a string or number",
    };
  },
});

export type ForeignKeyAction = "no action" | "restrict" | "cascade" | "set null" | "set default";

const FOREIGN_KEY_ACTIONS: ForeignKeyAction[] = ["no action", "restrict", "cascade", "set null", "set default"];

export const normalizeForeignKeyAction = (value: unknown): ForeignKeyAction => {
  const normalized = String(value ?? "no action").toLowerCase() as ForeignKeyAction;
  return FOREIGN_KEY_ACTIONS.includes(normalized) ? normalized : "no action";
};

/*export const autoIncrement = createMetaType<{}>({
  id: "autoIncrement",
  check: (type, options) => {
    if (isZodTypeExtends(type, zod.ZodNumber)) {
      return {
        success: true,
      };
    }
    return {
      success: false,
      message: "Primary key must be a number when using 'autoIncrement'",
    };
  },
});*/

export const primaryKey = createMetaType<{}>({
  id: "primaryKey",
  check: (type) => {
    if (isZodTypeExtends(type, zod.ZodString) || isZodTypeExtends(type, zod.ZodNumber)) {
      return {
        success: true,
      };
    }
    return {
      success: false,
      message: "Primary key must be a string or number",
    };
  },
});
export const updatedAt = createMetaType({
  id: "updatedAt",
});

export interface BackfillOptions {
  value: any;
}

export const backfill = createMetaType<BackfillOptions>({
  id: "backfill",
});
