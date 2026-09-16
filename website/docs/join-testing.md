---
title: Join and include test coverage
description: Cross-database regression contracts and upstream ORM test-suite inspiration.
---

The execution suite is `tests/joins.test.ts`, with inferred-type contracts in
`tests/types.contract.ts`. It executes the same behavior against Bun SQLite, Turso,
PostgreSQL, CockroachDB, MySQL and MariaDB. SQL Server dialect checks live in
`tests/providerAdaptors.test.ts`; live SQL Server checks require `MSSQL_TEST_URL`.

The following upstream suites informed the cases; their test implementations were
not copied:

| Suite | Cases carried into Zodbase's contracts |
| --- | --- |
| [Drizzle PostgreSQL](https://github.com/drizzle-team/drizzle-orm/blob/main/integration-tests/tests/pg/pg-common.ts) | Grouped/flat/partial selections, aliases, missing joined rows, unjoined field references, zero limits |
| [Kysely joins](https://github.com/kysely-org/kysely/blob/master/test/node/src/join.test.ts) | Explicit inner/left joins, compound ON conditions, SQL row semantics |
| [Sequelize eager loading](https://github.com/sequelize/sequelize/blob/main/packages/core/test/integration/include/findAll.test.js) | Nested includes, per-parent child limits, filters, ordering and sibling collections |
| [TypeORM joins](https://github.com/typeorm/typeorm/blob/master/test/functional/query-builder/join/query-builder-joins.test.ts) | Empty results, partial selection without a primary key, aliases and pagination |

Additional Zodbase contracts cover exactly one statement, cross-region/owner
predicates, reusable builder snapshots, exact bigint and UTC-date decoding,
JSON/boolean decoding, injection-safe values, duplicate scope rejection and
compile-time nullability. Explicit joins deliberately retain ordinary SQL
semantics rather than TypeORM's entity deduplication rules.
