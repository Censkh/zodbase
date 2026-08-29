import BunDatabase from "bun:sqlite";
import { createClient } from "@libsql/client";
import { CockroachDbContainer, type StartedCockroachDbContainer } from "@testcontainers/cockroachdb";
import { MySqlContainer, type StartedMySqlContainer } from "@testcontainers/mysql";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { createConnection } from "mysql2/promise";
import { Client } from "pg";
import type { Database as DatabaseApi } from "../../src";
import { Database } from "../../src";
import BunSqliteAdaptor from "../../src/adaptors/bun-sqlite";
import CockroachAdaptor from "../../src/adaptors/cockroach";
import MysqlAdaptor from "../../src/adaptors/mysql";
import PostgresAdaptor from "../../src/adaptors/postgres";
import TursoAdaptor from "../../src/adaptors/turso";

export interface TestDatabaseContext {
  db: DatabaseApi;
  close(): Promise<void>;
}

export interface TestDatabaseFactory {
  name: string;
  create(): Promise<TestDatabaseContext>;
}

const createBunSqliteDatabase = async (): Promise<TestDatabaseContext> => {
  const driver = new BunDatabase(":memory:");
  return {
    db: new Database({
      adaptor: new BunSqliteAdaptor({ driver }),
    }),
    async close() {
      driver.close();
    },
  };
};

const createTursoDatabase = async (): Promise<TestDatabaseContext> => {
  const driver = createClient({ url: ":memory:" });
  return {
    db: new Database({ adaptor: new TursoAdaptor({ driver }) }),
    async close() {
      driver.close();
    },
  };
};

const createPostgresDatabase = async (): Promise<TestDatabaseContext> => {
  if (!postgresContainer) {
    throw new Error("PostgreSQL test container has not been started");
  }

  const databaseName = `zodbase_${crypto.randomUUID().replaceAll("-", "")}`;
  const adminClient = new Client({ connectionString: postgresContainer.getConnectionUri() });
  await adminClient.connect();
  await adminClient.query(`CREATE DATABASE "${databaseName}"`);
  await adminClient.end();

  const connectionUrl = new URL(postgresContainer.getConnectionUri());
  connectionUrl.pathname = `/${databaseName}`;
  const driver = new Client({ connectionString: connectionUrl.toString() });
  await driver.connect();

  return {
    db: new Database({
      adaptor: new PostgresAdaptor({ driver }),
    }),
    async close() {
      await driver.end();
      const cleanupClient = new Client({ connectionString: postgresContainer?.getConnectionUri() });
      await cleanupClient.connect();
      await cleanupClient.query(`DROP DATABASE IF EXISTS "${databaseName}" WITH (FORCE)`);
      await cleanupClient.end();
    },
  };
};

const createMysqlDatabase = async (container: StartedMySqlContainer | undefined): Promise<TestDatabaseContext> => {
  if (!container) {
    throw new Error("MySQL-compatible test container has not been started");
  }

  const databaseName = `zodbase_${crypto.randomUUID().replace(/-/g, "")}`;
  const adminDriver = await createConnection({
    host: container.getHost(),
    port: container.getPort(),
    user: "root",
    password: container.getRootPassword(),
  });
  await adminDriver.query(`CREATE DATABASE \`${databaseName}\``);
  await adminDriver.end();

  const driver = await createConnection({
    host: container.getHost(),
    port: container.getPort(),
    user: "root",
    password: container.getRootPassword(),
    database: databaseName,
  });

  return {
    db: new Database({ adaptor: new MysqlAdaptor({ driver }) }),
    async close() {
      await driver.end();
      const cleanupDriver = await createConnection({
        host: container.getHost(),
        port: container.getPort(),
        user: "root",
        password: container.getRootPassword(),
      });
      await cleanupDriver.query(`DROP DATABASE IF EXISTS \`${databaseName}\``);
      await cleanupDriver.end();
    },
  };
};

const createCockroachDatabase = async (): Promise<TestDatabaseContext> => {
  if (!cockroachContainer) {
    throw new Error("CockroachDB test container has not been started");
  }

  const databaseName = `zodbase_${crypto.randomUUID().replace(/-/g, "")}`;
  const adminDriver = new Client({ connectionString: cockroachContainer.getConnectionUri() });
  await adminDriver.connect();
  try {
    await adminDriver.query(`CREATE DATABASE "${databaseName}"`);
  } finally {
    await adminDriver.end();
  }

  const connectionUrl = new URL(cockroachContainer.getConnectionUri());
  connectionUrl.pathname = `/${databaseName}`;
  const driver = new Client({ connectionString: connectionUrl.toString() });
  await driver.connect();

  return {
    db: new Database({ adaptor: new CockroachAdaptor({ driver }) }),
    async close() {
      await driver.end();
      const cleanupDriver = new Client({ connectionString: cockroachContainer?.getConnectionUri() });
      await cleanupDriver.connect();
      await cleanupDriver.query(`DROP DATABASE IF EXISTS "${databaseName}" CASCADE`);
      await cleanupDriver.end();
    },
  };
};

let postgresContainer: StartedPostgreSqlContainer | undefined;
let postgresContainerPromise: Promise<StartedPostgreSqlContainer> | undefined;
let postgresSuiteLeases = 0;
let mysqlContainer: StartedMySqlContainer | undefined;
let mysqlContainerPromise: Promise<StartedMySqlContainer> | undefined;
let mariadbContainer: StartedMySqlContainer | undefined;
let mariadbContainerPromise: Promise<StartedMySqlContainer> | undefined;
let mysqlSuiteLeases = 0;
let cockroachContainer: StartedCockroachDbContainer | undefined;
let cockroachContainerPromise: Promise<StartedCockroachDbContainer> | undefined;
let cockroachSuiteLeases = 0;

export const acquirePostgresTestContainer = async (): Promise<void> => {
  process.env.TESTCONTAINERS_RYUK_DISABLED ??= "true";
  postgresSuiteLeases += 1;
  postgresContainerPromise ??= new PostgreSqlContainer("postgres:17-alpine").start();
  postgresContainer = await postgresContainerPromise;
};

export const releasePostgresTestContainer = async (): Promise<void> => {
  postgresSuiteLeases -= 1;
  if (postgresSuiteLeases === 0 && postgresContainer) {
    await postgresContainer.stop();
    postgresContainer = undefined;
    postgresContainerPromise = undefined;
  }
};

export const acquireMysqlTestContainers = async (): Promise<void> => {
  process.env.TESTCONTAINERS_RYUK_DISABLED ??= "true";
  mysqlSuiteLeases += 1;
  mysqlContainerPromise ??= new MySqlContainer("mysql:8.4")
    .withDatabase("test")
    .withUsername("test")
    .withUserPassword("test-password")
    .withRootPassword("root-password")
    .start();
  mariadbContainerPromise ??= new MySqlContainer("mariadb:11.8")
    .withDatabase("test")
    .withUsername("test")
    .withUserPassword("test-password")
    .withRootPassword("root-password")
    .start();
  [mysqlContainer, mariadbContainer] = await Promise.all([mysqlContainerPromise, mariadbContainerPromise]);
};

export const releaseMysqlTestContainers = async (): Promise<void> => {
  mysqlSuiteLeases -= 1;
  if (mysqlSuiteLeases === 0) {
    await Promise.all([mysqlContainer?.stop(), mariadbContainer?.stop()]);
    mysqlContainer = undefined;
    mysqlContainerPromise = undefined;
    mariadbContainer = undefined;
    mariadbContainerPromise = undefined;
  }
};

export const acquireCockroachTestContainer = async (): Promise<void> => {
  process.env.TESTCONTAINERS_RYUK_DISABLED ??= "true";
  cockroachSuiteLeases += 1;
  cockroachContainerPromise ??= new CockroachDbContainer("cockroachdb/cockroach:v26.2.2")
    .withDatabase("defaultdb")
    .withUsername("root")
    .start();
  cockroachContainer = await cockroachContainerPromise;
};

export const releaseCockroachTestContainer = async (): Promise<void> => {
  cockroachSuiteLeases -= 1;
  if (cockroachSuiteLeases === 0 && cockroachContainer) {
    await cockroachContainer.stop();
    cockroachContainer = undefined;
    cockroachContainerPromise = undefined;
  }
};

export const acquireTestDatabaseContainers = async (): Promise<void> => {
  await Promise.all([acquirePostgresTestContainer(), acquireMysqlTestContainers(), acquireCockroachTestContainer()]);
};

export const releaseTestDatabaseContainers = async (): Promise<void> => {
  await Promise.all([releasePostgresTestContainer(), releaseMysqlTestContainers(), releaseCockroachTestContainer()]);
};

export const TEST_DATABASE_FACTORIES: TestDatabaseFactory[] = [
  { name: "bun-sqlite", create: createBunSqliteDatabase },
  { name: "turso-local", create: createTursoDatabase },
  { name: "postgres", create: createPostgresDatabase },
  { name: "cockroach", create: createCockroachDatabase },
  { name: "mysql", create: () => createMysqlDatabase(mysqlContainer) },
  { name: "mariadb", create: () => createMysqlDatabase(mariadbContainer) },
];
