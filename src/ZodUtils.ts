import * as zod from "zod";
import type { Class } from "./Types";

export const isZodTypeExtends = (type: zod.ZodType, zodType: Class<zod.ZodType>): zod.ZodType | false => {
  if (type instanceof zodType) {
    return type;
  }
  // @ts-ignore
  if (type.def.type === "union") {
    // @ts-ignore
    for (const option of type.def.options) {
      if (isZodTypeExtends(option, zodType)) {
        return option;
      }
    }
    return false;
  }
  // @ts-ignore
  const rootType = type.def.innerType;
  if (rootType) {
    return isZodTypeExtends(rootType, zodType);
  }
  return false;
};

export const isZodRequired = (type: zod.ZodType): boolean => {
  return !isZodTypeExtends(type, zod.ZodOptional) && !isZodTypeExtends(type, zod.ZodNullable);
};
