import BunDatabase from "bun:sqlite";
import { rm } from "node:fs/promises";
import { createClient } from "@libsql/client";
import { CockroachDbContainer, type StartedCockroachDbContainer } from "@testcontainers/cockroachdb";
import { MySqlContainer, type StartedMySqlContainer } from "@testcontainers/mysql";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { type ConnectionOptions, createConnection } from "mysql2/promise";
import { Client } from "pg";
import type { Database as DatabaseApi } from "../../src";
import { Database } from "../../src";
import BunSqliteAdaptor from "../../src/adaptors/bun-sqlite";
import CockroachAdaptor from "../../src/adaptors/cockroach";
import MariaDbAdaptor from "../../src/adaptors/mariadb";
import MysqlAdaptor from "../../src/adaptors/mysql";
import PostgresAdaptor from "../../src/adaptors/postgres";
import TursoAdaptor from "../../src/adaptors/turso";

interface SharedTestDatabases {
  postgres: string;
  cockroach: string;
  mysql: ConnectionOptions;
  mariadb: ConnectionOptions;
}

// Only the parent runner owns these disposable containers. Each test still gets its own database.
const sharedDatabases: SharedTestDatabases | undefined = process.env.ZODBASE_TEST_DATABASES
  ? JSON.parse(process.env.ZODBASE_TEST_DATABASES)
  : undefined;

export interface TestDatabaseContext {
  db: DatabaseApi;
  connect?(): Promise<TestDatabaseContext>;
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
  const databasePath = `/tmp/zodbase_${crypto.randomUUID()}.db`;
  const driver = createClient({ url: `file:${databasePath}` });
  return {
    db: new Database({ adaptor: new TursoAdaptor({ driver }) }),
    async close() {
      driver.close();
      await Promise.all([
        rm(databasePath, { force: true }),
        rm(`${databasePath}-shm`, { force: true }),
        rm(`${databasePath}-wal`, { force: true }),
      ]);
    },
  };
};

const createPostgresDatabase = async (): Promise<TestDatabaseContext> => {
  const uri = sharedDatabases?.postgres ?? postgresContainer?.getConnectionUri();
  if (!uri) {
    throw new Error("PostgreSQL test container has not been started");
  }

  const databaseName = `zodbase_${crypto.randomUUID().replaceAll("-", "")}`;
  const adminClient = new Client({ connectionString: uri });
  await adminClient.connect();
  await adminClient.query(`CREATE DATABASE "${databaseName}"`);
  await adminClient.end();

  const connectionUrl = new URL(uri);
  connectionUrl.pathname = `/${databaseName}`;
  const driver = new Client({ connectionString: connectionUrl.toString() });
  await driver.connect();

  return {
    db: new Database({
      adaptor: new PostgresAdaptor({ driver }),
    }),
    async connect() {
      const connection = new Client({ connectionString: connectionUrl.toString() });
      await connection.connect();
      return {
        db: new Database({ adaptor: new PostgresAdaptor({ driver: connection }) }),
        close: () => connection.end(),
      };
    },
    async close() {
      await driver.end();
      const cleanupClient = new Client({ connectionString: uri });
      await cleanupClient.connect();
      await cleanupClient.query(`DROP DATABASE IF EXISTS "${databaseName}" WITH (FORCE)`);
      await cleanupClient.end();
    },
  };
};

const mysqlOptions = (container: StartedMySqlContainer): ConnectionOptions => ({
  host: container.getHost(),
  port: container.getPort(),
  user: "root",
  password: container.getRootPassword(),
});

const createMysqlDatabase = async (engine: "mysql" | "mariadb"): Promise<TestDatabaseContext> => {
  const container = engine === "mariadb" ? mariadbContainer : mysqlContainer;
  const options = sharedDatabases?.[engine] ?? (container && mysqlOptions(container));
  if (!options) throw new Error("MySQL-compatible test container has not been started");

  const databaseName = `zodbase_${crypto.randomUUID().replaceAll("-", "")}`;
  const adminDriver = await createConnection(options);
  try {
    await adminDriver.query(`CREATE DATABASE \`${databaseName}\``);
  } finally {
    await adminDriver.end();
  }
  const driver = await createConnection({ ...options, database: databaseName });
  return {
    db: new Database({ adaptor: new (engine === "mariadb" ? MariaDbAdaptor : MysqlAdaptor)({ driver }) }),
    async close() {
      await driver.end();
      const cleanupDriver = await createConnection(options);
      try {
        await cleanupDriver.query(`DROP DATABASE IF EXISTS \`${databaseName}\``);
      } finally {
        await cleanupDriver.end();
      }
    },
  };
};

const createCockroachDatabase = async (): Promise<TestDatabaseContext> => {
  const uri = sharedDatabases?.cockroach ?? cockroachContainer?.getConnectionUri();
  if (!uri) {
    throw new Error("CockroachDB test container has not been started");
  }

  const databaseName = `zodbase_${crypto.randomUUID().replace(/-/g, "")}`;
  const adminDriver = new Client({ connectionString: uri });
  await adminDriver.connect();
  try {
    await adminDriver.query(`CREATE DATABASE "${databaseName}"`);
  } finally {
    await adminDriver.end();
  }

  const connectionUrl = new URL(uri);
  connectionUrl.pathname = `/${databaseName}`;
  const driver = new Client({ connectionString: connectionUrl.toString() });
  await driver.connect();

  return {
    db: new Database({ adaptor: new CockroachAdaptor({ driver }) }),
    async connect() {
      const connection = new Client({ connectionString: connectionUrl.toString() });
      await connection.connect();
      return {
        db: new Database({ adaptor: new CockroachAdaptor({ driver: connection }) }),
        close: () => connection.end(),
      };
    },
    async close() {
      await driver.end();
      const cleanupDriver = new Client({ connectionString: uri });
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
  if (sharedDatabases) return;
  process.env.TESTCONTAINERS_RYUK_DISABLED ??= "true";
  postgresSuiteLeases += 1;
  postgresContainerPromise ??= new PostgreSqlContainer("postgres:17-alpine").withStartupTimeout(120_000).start();
  postgresContainer = await postgresContainerPromise;
};

export const releasePostgresTestContainer = async (): Promise<void> => {
  if (sharedDatabases) return;
  postgresSuiteLeases -= 1;
  if (postgresSuiteLeases === 0 && postgresContainer) {
    await postgresContainer.stop();
    postgresContainer = undefined;
    postgresContainerPromise = undefined;
  }
};

export const acquireMysqlTestContainers = async (): Promise<void> => {
  if (sharedDatabases) return;
  process.env.TESTCONTAINERS_RYUK_DISABLED ??= "true";
  mysqlSuiteLeases += 1;
  mysqlContainerPromise ??= new MySqlContainer("mysql:8.4")
    .withDatabase("test")
    .withUsername("test")
    .withUserPassword("test-password")
    .withRootPassword("root-password")
    .withStartupTimeout(120_000)
    .start();
  mariadbContainerPromise ??= new MySqlContainer("mariadb:11.8")
    .withDatabase("test")
    .withUsername("test")
    .withUserPassword("test-password")
    .withRootPassword("root-password")
    .withStartupTimeout(120_000)
    .start();
  const results = await Promise.allSettled([
    mysqlContainerPromise.then((container) => {
      mysqlContainer = container;
    }),
    mariadbContainerPromise.then((container) => {
      mariadbContainer = container;
    }),
  ]);
  for (const result of results) if (result.status === "rejected") throw result.reason;
};

export const releaseMysqlTestContainers = async (): Promise<void> => {
  if (sharedDatabases) return;
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
  if (sharedDatabases) return;
  process.env.TESTCONTAINERS_RYUK_DISABLED ??= "true";
  cockroachSuiteLeases += 1;
  cockroachContainerPromise ??= new CockroachDbContainer("cockroachdb/cockroach:v26.2.2")
    .withCommand([
      "start-single-node",
      "--insecure",
      "--http-addr=0.0.0.0:26258",
      "--locality=region=aws-ap-southeast-1,zone=a",
    ])
    .withDatabase("defaultdb")
    .withUsername("root")
    .withStartupTimeout(120_000)
    .start();
  cockroachContainer = await cockroachContainerPromise;
};

export const releaseCockroachTestContainer = async (): Promise<void> => {
  if (sharedDatabases) return;
  cockroachSuiteLeases -= 1;
  if (cockroachSuiteLeases === 0 && cockroachContainer) {
    await cockroachContainer.stop();
    cockroachContainer = undefined;
    cockroachContainerPromise = undefined;
  }
};

export const acquireTestDatabaseContainers = async (): Promise<void> => {
  const results = await Promise.allSettled([
    acquirePostgresTestContainer(),
    acquireMysqlTestContainers(),
    acquireCockroachTestContainer(),
  ]);
  for (const result of results) if (result.status === "rejected") throw result.reason;
};

export const releaseTestDatabaseContainers = async (): Promise<void> => {
  await Promise.all([releasePostgresTestContainer(), releaseMysqlTestContainers(), releaseCockroachTestContainer()]);
};

export const TEST_DATABASE_FACTORIES: TestDatabaseFactory[] = [
  { name: "bun-sqlite", create: createBunSqliteDatabase },
  { name: "turso-local", create: createTursoDatabase },
  { name: "postgres", create: createPostgresDatabase },
  { name: "cockroach", create: createCockroachDatabase },
  { name: "mysql", create: () => createMysqlDatabase("mysql") },
  { name: "mariadb", create: () => createMysqlDatabase("mariadb") },
];

export const getSharedTestDatabases = (): string => {
  if (!postgresContainer || !cockroachContainer || !mysqlContainer || !mariadbContainer) {
    throw new Error("Test containers must be started before sharing connection details");
  }
  return JSON.stringify({
    postgres: postgresContainer.getConnectionUri(),
    cockroach: cockroachContainer.getConnectionUri(),
    mysql: mysqlOptions(mysqlContainer),
    mariadb: mysqlOptions(mariadbContainer),
  } satisfies SharedTestDatabases);
};
