---
title: Subquery test contracts
description: Regression coverage and upstream test-suite inspiration for scalar insert subqueries.
---

The suite combines dialect SQL assertions, real database execution and compile-time type checks. It takes the testing approach, rather than implementation code, from [Kysely's insert suite](https://github.com/kysely-org/kysely/blob/master/test/node/src/insert.test.ts) (mixed expressions, defaults and dialect output) and [SQLAlchemy's insert compiler suite](https://github.com/sqlalchemy/sqlalchemy/blob/main/test/sql/test_insert.py) (column/value mapping, expressions and malformed inputs).

| Contract | Tests |
| --- | --- |
| One SQL statement, defaults, multiple computed fields, native date values, missing required parent | `subquery.test.ts` across six engines |
| NULL/missing results, owner predicates, ordered offset, zero limit, mixed bulk column order, atomic failures and transactional visibility | `subqueryDialectEdges.test.ts` across six engines |
| Construction laziness, concurrent awaits, escaping/Unicode, query reuse, invalid literals/projections, failed-promise reuse and rollback | `subqueryEdges.test.ts` on actual SQLite |
| Shared deterministic point reads; no sharing for volatile, ordered or self-referencing queries | `subquerySharing.test.ts` |
| Indexed point plans; PostgreSQL shared InitPlan; Cockroach fast auto-commit | `subqueryPlan.test.ts` using actual EXPLAIN |
| Projection types, forbidden multi-column/wildcard inputs, actual provider driver compatibility | `types.contract.ts` |
| SQL Server OUTPUT, pagination, exact values and native transaction driver calls; Neon HTTP contracts | `providerAdaptors.test.ts` |
| Actual Neon driver wire decoding and mode overrides with a mocked HTTP transport | `neonDriver.test.ts` |
| SQL Server CRUD, subqueries, indexes/FKs, rollback and schema migration atomicity | `mssql.integration.test.ts`, native x86 CI |

PostgreSQL, CockroachDB, MySQL/MariaDB and SQL Server reject multi-row scalar results. SQLite uses its native first-row scalar semantics. Queries that can return multiple rows should specify a meaningful order and `limit(1)` when selecting a single value is intended. Missing rows produce SQL NULL, not a JavaScript/Zod default.

The SQL Server integration suite is conditional on `MSSQL_TEST_URL`; other engine suites use Testcontainers. A skipped suite or compiler snapshot is not equivalent to a live database pass. Hosted cloud accounts are not provisioned by these tests.
