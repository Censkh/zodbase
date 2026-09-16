---
title: Cloud database providers
description: Map managed database products to their actual Zodbase dialect and driver.
---

Choose the adaptor for the database engine and connection protocol, not the hosting brand. A provider URL alone does not change SQL syntax. The mappings below describe protocol compatibility; they are not claims that every hosted service has been exercised by the test suite.

| Service / engine | Adaptor | Connection |
| --- | --- | --- |
| Supabase PostgreSQL | `postgres` | `pg` using the database or pooler connection string, not the REST API |
| Neon PostgreSQL | `postgres` or `neon-http` | Neon WebSocket `Pool`/`Client`, or `neon()` HTTP driver |
| AWS RDS PostgreSQL / Aurora PostgreSQL | `postgres` | `pg` with the writer endpoint |
| AWS RDS MySQL / MariaDB / Aurora MySQL | `mysql` | `mysql2/promise` with the writer endpoint |
| AWS RDS SQL Server | `mssql` (experimental) | `mssql` |
| Google Cloud SQL PostgreSQL / AlloyDB | `postgres` | `pg`, optionally through the provider connector/proxy |
| Google Cloud SQL MySQL | `mysql` | `mysql2/promise` |
| Google Cloud SQL SQL Server | `mssql` (experimental) | `mssql` |
| Azure Database for PostgreSQL / MySQL | `postgres` / `mysql` | Corresponding native driver |
| Azure SQL Database / Managed Instance | `mssql` (experimental) | `mssql` |
| Cloudflare D1 | `d1` | Worker binding |
| Cloudflare Hyperdrive | Underlying `postgres` or `mysql` engine | Native driver configured with the binding's connection string |
| Turso / libSQL | `turso` | `@libsql/client` |

TLS, IAM/Entra authentication, proxies, private networks and connection lifetime belong in the driver/provider configuration. Use a writer-capable endpoint for migrations and mutations. Aurora's HTTP Data API is a different transport and is **not** covered by the PostgreSQL TCP adaptor.

## Cosmos DB, DynamoDB, Firestore and Spanner

Cosmos DB's NoSQL/document API, DynamoDB and Firestore are not relational SQL engines. Their partition-scoped transactions, query restrictions and indexing models require dedicated APIs and contracts; there is no supported adaptor for them here. Cosmos DB's SQL-like query language does not make relational subquery writes portable.

Spanner has its own execution, schema and transaction semantics, even when using its PostgreSQL interface. Do not route it through `postgres` and assume schema sync or the subquery SQL is supported. It needs a dedicated tested adaptor. Oracle and Db2 engines are also not currently supported.

Provider documentation: [AWS Aurora compatibility](https://aws.amazon.com/rds/aurora/), [RDS engines](https://aws.amazon.com/rds/features/), [Cloud SQL engines](https://docs.cloud.google.com/sql/docs), [Spanner PostgreSQL interface](https://docs.cloud.google.com/spanner/docs/postgresql-interface), [Cosmos DB overview](https://learn.microsoft.com/en-us/azure/cosmos-db/overview).
