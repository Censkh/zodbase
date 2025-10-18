export { metaStore } from "zod-meta";
export * from "./index.common";

const noop = () => {};

export const foreignKey = noop;
export const primaryKey = noop;
export const updatedAt = noop;
export const backfill = noop;

export class Database {}

export const sql = noop;
