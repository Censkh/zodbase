---
title: RSQL filters
description: Convert a supported subset of RSQL filter expressions into typed Zodbase query conditions.
---

Zodbase's optional RSQL module converts supported filter expressions into table conditions. Install its parser dependency when using this entry point:

```bash
bun add @rsql/parser
```

## Apply a filter

```ts
import { rsqlToCondition } from "zodbase/rsql";

const condition = rsqlToCondition(Users, "name==Ada");
const query = db.select(Users);
const { results } = await (condition ? query.where(condition) : query);
```

An empty filter returns `undefined`. Malformed syntax throws an error.

## Restrict fields and operators

Without options, a filter may name any field of the table. Before exposing filtering to callers, list the selectors they may use, each with the field it reads, and the operators they may use:

```ts
const condition = rsqlToCondition(Users, filter, {
  fields: { name: "name", "profile.country": "country" },
  operators: ["==", "!=", "=in=", "=out="],
});
```

A selector the `fields` map leaves out throws, including the table field behind an allowed selector (`country` above), so a filter can't probe columns your application doesn't expose. An operator `operators` leaves out throws too.

## Supported expressions

The module implements a subset of RSQL. Refer to the [RSQL tests](https://github.com/Censkh/zodbase/blob/master/tests/rsql.test.ts) for supported operators, logical combinations, arrays, and null handling. Do not assume every expression supported by an RSQL parser is implemented by Zodbase.

## Operator examples

| Filter | Interpretation |
| --- | --- |
| `name==Ada` | Equal to a string. |
| `active==true` | Boolean equality, coerced using the field schema. |
| `quantity>=20` | Numeric comparison for a numeric field. |
| `id=in=(ada,grace)` | Match one of several values. |
| `id=out=(ada,grace)` | Exclude several values. |
| `name==Ada*` | `*` in an `==` or `!=` string value matches any run of characters, case-insensitively. Other characters, including `%` and `_`, match themselves. |
| `name=like=A%` | SQL LIKE matching: `%` and `_` are LIKE wildcards, and case sensitivity follows the database. Prefer `*` in `==` for consistent, case-insensitive matching. |
| `parentId==null` | Null equality for a nullable field. |
| `active==true;quantity>=20` | AND. |
| `active==false,quantity==10` | OR. |
| `(active==true,quantity==20);parentId==null` | Explicit grouping. |

These field names illustrate a schema that declares the corresponding types; only fields present on the supplied table are valid. Supported comparisons include `==`, `!=`, `>`, `>=`, `<`, and `<=`; the parser also accepts its verbose logical syntax.

## Arrays and quoted values

For a JSON-array field, `tags=in=(keep,urgent)` checks exact element membership, not a substring such as `keep-old`. Quote values containing separators, for example `name=="Ada, Lovelace"`. When sending a filter as a URL query parameter, let `URLSearchParams` encode the complete expression.

## Failure behavior

Unknown fields, unsupported operators, and invalid coercions throw an `RsqlFilterError`, with the message `Failed to parse RSQL filter: …`. Its `field` names the selector when the filter names a field it can't use. In a request handler, translate a filter error into an appropriate client error; do not silently drop the filter and return an unfiltered dataset.

The converter does not enforce authorization, query limits, or your application's filter policy. Apply those independently. Do not pass the filter through `raw`; pass the resulting condition to the typed query builder.
