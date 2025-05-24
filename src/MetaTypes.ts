import * as zod from "zod/v4";
import { createMetaType } from "zod-meta";
import type { SingleFieldBinding } from "./QueryBuilder";
import { isZodTypeExtends } from "./ZodUtils";

export const foreignKey = createMetaType<{
  field: SingleFieldBinding;
}>({
  id: "foreignKey",
  check: (type, options) => {
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
  check: (type, options) => {
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
