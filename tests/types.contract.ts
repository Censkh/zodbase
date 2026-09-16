import * as zod from "zod";
import { createTable, type Database, expression, type InputOfTable, sql } from "../src";
import type { SqlResult } from "../src/QueryBuilder";

type Equal<TLeft, TRight> = (<T>() => T extends TLeft ? 1 : 2) extends <T>() => T extends TRight ? 1 : 2 ? true : false;
type Expect<TValue extends true> = TValue;

declare const db: Database;

const PeopleTable = createTable({
  id: "type_people",
  schema: zod.object({
    id: zod.string(),
    name: zod.string(),
    age: zod.number(),
    nickname: zod.string().optional(),
  }),
});

export type PeopleInputContract = Expect<
  Equal<InputOfTable<typeof PeopleTable>, { id: string; name: string; age: number; nickname?: string | undefined }>
>;

const selectedIds: PromiseLike<SqlResult<{ id: string }, number>> = db.select(PeopleTable, { id: PeopleTable.$id });

const selectedOne: PromiseLike<SqlResult<{ id: string; name: string }, 1>> = db
  .select(PeopleTable, { id: PeopleTable.$id, name: PeopleTable.$name })
  .one();

void selectedIds;
void selectedOne;

PeopleTable.$age.greaterThan(18);
PeopleTable.$nickname.notEquals(null);
db.insert(PeopleTable, { id: "1", name: "Ada", age: 36 });
db.update(PeopleTable, { nickname: "Countess" }, PeopleTable.$id.equals("1"));

// @ts-expect-error unknown fields cannot be selected
db.select(PeopleTable, { missing: PeopleTable.$missing });
// @ts-expect-error conditions require the field's value type
PeopleTable.$age.equals("old");
// @ts-expect-error required insert fields cannot be omitted
db.insert(PeopleTable, { id: "1", name: "Ada" });
// @ts-expect-error update values must match the schema
db.update(PeopleTable, { age: "old" }, PeopleTable.$id.equals("1"));

// Scalar SELECTs retain their selected value type through chaining.
db.insert(PeopleTable, {
  id: "subquery",
  name: db.select(PeopleTable, { name: PeopleTable.$name }).where(PeopleTable.$id.equals("1")),
  age: 1,
}).selectMutated();
db.insertMany(PeopleTable, [{ id: "many", name: "Ada", age: db.select(PeopleTable, { age: PeopleTable.$age }).one() }]);
// @ts-expect-error numeric projections cannot fill text columns
db.insert(PeopleTable, { id: "wrong", name: db.select(PeopleTable, { age: PeopleTable.$age }), age: 1 });
db.insert(PeopleTable, {
  id: "wide",
  // @ts-expect-error multiple selected columns are not scalar values
  name: db.select(PeopleTable, { id: PeopleTable.$id, name: PeopleTable.$name }),
  age: 1,
});
db.insert(PeopleTable, {
  id: "star",
  // @ts-expect-error SELECT * is not a scalar projection
  name: db.select(PeopleTable),
  age: 1,
});
db.insert(PeopleTable, {
  id: "parsed",
  name: db.select(PeopleTable, { name: PeopleTable.$name }),
  age: 1,
  // @ts-expect-error parsed inputs cannot expose a database-computed value
}).selectParsed();

// Real provider driver types must work without casts or bundled runtime dependencies.
import type { Client as NeonClient, Pool as NeonPool, neon } from "@neondatabase/serverless";
import type { ConnectionPool } from "mssql";
import MssqlAdaptor from "../src/adaptors/mssql";
import NeonHttpAdaptor from "../src/adaptors/neon-http";
import PostgresAdaptor from "../src/adaptors/postgres";

declare const neonHttp: ReturnType<typeof neon>;
declare const neonClient: NeonClient;
declare const neonPool: NeonPool;
declare const mssqlPool: ConnectionPool;
new NeonHttpAdaptor({ driver: neonHttp });
new PostgresAdaptor({ driver: neonClient });
new PostgresAdaptor({ driver: neonPool });
new MssqlAdaptor({ driver: mssqlPool });

const NullableSource = createTable({
  id: "nullable_source",
  schema: zod.object({ value: zod.string().nullable(), timestamp: zod.date() }),
});
const NullableTarget = createTable({
  id: "nullable_target",
  schema: zod.object({ value: zod.string().nullable(), timestamp: zod.date() }),
});
db.insert(NullableTarget, {
  value: db.select(NullableSource, { value: NullableSource.$value }),
  timestamp: db.select(NullableSource, { timestamp: NullableSource.$timestamp }),
});
// @ts-expect-error a nullable projection cannot fill a non-nullable text column
db.insert(PeopleTable, { id: "nullable", name: db.select(NullableSource, { value: NullableSource.$value }), age: 1 });
db.insert(PeopleTable, {
  id: "date",
  // @ts-expect-error a date projection cannot fill a text column
  name: db.select(NullableSource, { timestamp: NullableSource.$timestamp }),
  age: 1,
});
const narrowed = db.select(PeopleTable, { name: PeopleTable.$name }).clone().limit(1);
db.insert(PeopleTable, { id: "narrowed", name: narrowed, age: 1 });

const JoinedTable = createTable({ id: "joined_types", schema: zod.object({ id: zod.string(), age: zod.number() }) });
PeopleTable.$id.equals(JoinedTable.$id);
// @ts-expect-error column comparisons must have compatible value types
PeopleTable.$id.equals(JoinedTable.$age);
const joinedTyped = db
  .select(PeopleTable, { person: PeopleTable, joined: JoinedTable })
  .leftJoin(JoinedTable, JoinedTable.$id.equals(PeopleTable.$id));
export type JoinNullContract = Expect<
  Equal<Awaited<typeof joinedTyped>["results"][number]["joined"], { id: string; age: number } | null>
>;
const partialTyped = db
  .select(PeopleTable, { id: PeopleTable.$id, joined: { age: JoinedTable.$age } })
  .leftJoin(JoinedTable, JoinedTable.$id.equals(PeopleTable.$id));
export type PartialJoinNullContract = Expect<
  Equal<Awaited<typeof partialTyped>["results"][number]["joined"], { age: number } | null>
>;
const includedTyped = db.select(PeopleTable, { id: PeopleTable.$id }).include({
  children: db.select(JoinedTable, { id: JoinedTable.$id }).where(JoinedTable.$id.equals(PeopleTable.$id)),
});
export type IncludeContract = Expect<
  Equal<Awaited<typeof includedTyped>["results"][number]["children"], { id: string }[]>
>;
// @ts-expect-error includes require SELECT builders, not arbitrary promises
db.select(PeopleTable, { id: PeopleTable.$id }).include({ children: Promise.resolve([]) });

const fieldsBeforeJoin = db
  .select(PeopleTable, { joined: JoinedTable })
  .leftJoin(JoinedTable, JoinedTable.$id.equals(PeopleTable.$id));
export type LateJoinContract = Expect<
  Equal<Awaited<typeof fieldsBeforeJoin>["results"][number]["joined"], { id: string; age: number } | null>
>;
const mixedProjection = db
  .select(PeopleTable, { mixed: { root: PeopleTable.$id, joined: JoinedTable.$id } })
  .leftJoin(JoinedTable, JoinedTable.$id.equals(PeopleTable.$id));
export type MixedJoinContract = Expect<
  Equal<Awaited<typeof mixedProjection>["results"][number]["mixed"], { root: string; joined: string | null }>
>;

const computedProjection = db.select(PeopleTable, { value: expression(sql`1`, zod.number()) });
const computedContract: PromiseLike<SqlResult<{ value: number }, number>> = computedProjection;
void computedContract;

// Nullable foreign keys remain comparable to non-null keys.
PeopleTable.$name.equals(PeopleTable.$nickname);

const clonedInclude = includedTyped.clone().limit(1);
export type ClonedIncludeContract = Expect<
  Equal<Awaited<typeof clonedInclude>["results"][number]["children"], { id: string }[]>
>;
const replacedInclude = includedTyped.clone().include({ children: db.select(JoinedTable, { age: JoinedTable.$age }) });
export type ReplacedIncludeContract = Expect<
  Equal<Awaited<typeof replacedInclude>["results"][number]["children"], { age: number }[]>
>;

db.insert(PeopleTable, {
  id: "projected-scalar",
  name: db.select(PeopleTable, { name: PeopleTable.$name }),
  age: 1,
});
db.insert(PeopleTable, {
  id: "nested-projection",
  // @ts-expect-error nested result projections are not scalar SQL expressions
  name: db.select(PeopleTable, { person: { name: PeopleTable.$name } }),
  age: 1,
});
