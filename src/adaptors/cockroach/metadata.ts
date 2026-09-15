import { createMetaType } from "zod-meta";

/** CockroachDB table locality, attached to the table's Zod object schema. */
export const cockroachLocality = createMetaType<{
  type: "regional-by-row" | "regional-by-table" | "global";
  regionColumn?: string;
  region?: string;
}>({ id: "cockroachLocality" });

/** A string backed by CockroachDB's database-managed region enum. */
export const cockroachRegion = createMetaType({ id: "cockroachRegion" });
