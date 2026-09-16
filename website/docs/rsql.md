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

An empty filter returns `undefined`. Malformed syntax throws an error. Validate the fields and operators your application permits before exposing arbitrary filtering.

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
| `name=like=A%` | SQL LIKE matching. |
| `parentId==null` | Null equality for a nullable field. |
| `active==true;quantity>=20` | AND. |
| `active==false,quantity==10` | OR. |
| `(active==true,quantity==20);parentId==null` | Explicit grouping. |

These field names illustrate a schema that declares the corresponding types; only fields present on the supplied table are valid. Supported comparisons include `==`, `!=`, `>`, `>=`, `<`, and `<=`; the parser also accepts its verbose logical syntax.

## Arrays and quoted values

For a JSON-array field, `tags=in=(keep,urgent)` checks exact element membership, not a substring such as `keep-old`. Quote values containing separators, for example `name=="Ada, Lovelace"`. When sending a filter as a URL query parameter, let `URLSearchParams` encode the complete expression.

## Failure behavior

Unknown fields, unsupported operators, and invalid coercions throw. Errors are wrapped with `Failed to parse RSQL filter`. In a request handler, translate a filter error into an appropriate client error; do not silently drop the filter and return an unfiltered dataset.

The converter does not enforce authorization, query limits, or your application's filter policy. Apply those independently. Do not pass the filter through `raw`; pass the resulting condition to the typed query builder.
