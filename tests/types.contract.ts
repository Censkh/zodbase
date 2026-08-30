import * as zod from "zod";
import { createTable, type Database, type InputOfTable } from "../src";
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

const selectedIds: PromiseLike<SqlResult<{ id: string }, number>> = db.select(PeopleTable, ["id"]);

const selectedOne: PromiseLike<SqlResult<{ id: string; name: string }, 1>> = db
  .select(PeopleTable, ["id", "name"])
  .one();

void selectedIds;
void selectedOne;

PeopleTable.$age.greaterThan(18);
PeopleTable.$nickname.notEquals(null);
db.insert(PeopleTable, { id: "1", name: "Ada", age: 36 });
db.update(PeopleTable, { nickname: "Countess" }, PeopleTable.$id.equals("1"));

// @ts-expect-error unknown fields cannot be selected
db.select(PeopleTable, ["missing"]);
// @ts-expect-error conditions require the field's value type
PeopleTable.$age.equals("old");
// @ts-expect-error required insert fields cannot be omitted
db.insert(PeopleTable, { id: "1", name: "Ada" });
// @ts-expect-error update values must match the schema
db.update(PeopleTable, { age: "old" }, PeopleTable.$id.equals("1"));
