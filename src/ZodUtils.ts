import * as zod from "zod/v4";
import type { Class } from "./Types";

export const isZodTypeExtends = (type: zod.ZodType, zodType: Class<zod.ZodType>): zod.ZodType | false => {
  if (type instanceof zodType) {
    return type;
  }
  // @ts-expect-error
  if (type.def.type === "union") {
    // @ts-expect-error
    for (const option of type.def.options) {
      if (isZodTypeExtends(option, zodType)) {
        return option;
      }
    }
    return false;
  }
  // @ts-expect-error
  const rootType = type.def.innerType;
  if (rootType) {
    return isZodTypeExtends(rootType, zodType);
  }
  return false;
};

export const isZodRequired = (type: zod.ZodType): boolean => {
  return !isZodTypeExtends(type, zod.ZodOptional) && !isZodTypeExtends(type, zod.ZodNullable);
};
