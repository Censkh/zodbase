# Database contract coverage

Run `bun run test` and `bun run test:compiled` from this package. Docker is required.
The shared matrix uses actual Bun SQLite, local libSQL, PostgreSQL, CockroachDB,
MySQL and MariaDB. Compiled schemas run the same contracts.

| Area | Coverage |
| --- | --- |
| Values and driver decoding | `edgeCases.test.ts`: numbers (safe integer bounds, millisecond timestamps, fractions, very small/large values), int32 boundaries, Unicode/quotes/backslashes, JSON-looking text, booleans, dates with milliseconds, 64-bit bigints, objects, arrays, enums and explicit nulls. Insert, select, update-returning and upsert-returning all compare values and JS types. Non-finite numbers and invalid dates must fail before insertion. |
| Partial updates | `updateDefaults.test.ts` runs across all six engines: omitted fields remain unchanged, supplied undefined invokes declared defaults, explicit null survives, generated IDs are not regenerated, heterogeneous bulk updates preserve each row's untouched fields, invalid values do not write. |
| Concurrency | `edgeCases.test.ts`: duplicate primary-key races, monotonic updates and rollback on every engine; independent PostgreSQL/Cockroach connections race on the same row. The Cockroach test requires an actual serialization retry. `cockroachRetry.test.ts` covers bounded retries and error preservation. |
| Schema changes | Existing schema-contract suites cover backfills, indexes, foreign keys, constraints and idempotence. `edgeCases.test.ts` resumes after a column was added but not backfilled/constrained and migrates MySQL datetime precision. `numericPrecision.test.ts` migrates legacy indexed numeric columns and advances revisions beyond cached rounded values. |
| Ordering/pagination | `edgeCases.test.ts`: explicit timestamp + ID tie-breaker, keyset continuation with an insert between pages, nullable sort keys, null filters, offset without a limit. Existing database contracts cover limits, offsets, zero limits and independent query builders. Native default null ordering is tested per dialect; keyset callers must provide a total ordering. |
| Additional drivers | `adaptorContract.test.ts` exercises D1, Expo SQLite and better-sqlite3 API contracts with driver doubles. These are not substitutes for device/Worker integration tests. |

Raw `execute(sql)` returns driver values without guessing whether text is JSON.
Typed table reads decode using column schemas. Use explicit object/array schemas
for structured columns and string schemas for opaque text. Untyped legacy
`any`/`unknown` columns retain their historical structured-JSON decoding.
MySQL connections use `NO_BACKSLASH_ESCAPES` because SQL literals use standard
quote doubling. Date fields are stored/read as UTC with millisecond precision.

Sources of test ideas (cases adapted to zodbase's API, not copied wholesale):
- [Sequelize datatype tests](https://github.com/sequelize/sequelize/blob/main/packages/core/test/integration/data-types/data-types.test.ts)
- [SQLAlchemy dialect datatype contracts](https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/testing/suite/test_types.py)
- [Drizzle schema migration tests](https://github.com/drizzle-team/drizzle-orm/tree/main/drizzle-kit/tests)
- [TypeORM issue regressions](https://github.com/typeorm/typeorm/tree/master/test/github-issues)
