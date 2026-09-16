import {
  acquireTestDatabaseContainers,
  getSharedTestDatabases,
  releaseTestDatabaseContainers,
} from "../tests/helpers/databaseContract";

// Tests in each file share mutable hooks, so parallelize files, never individual tests.
const startedAt = performance.now();
const args = process.argv.slice(2);
const compiled = args.includes("--compiled");
const workers = process.env.ZODBASE_TEST_WORKERS ?? "2";
if (!/^[1-9]\d*$/.test(workers)) throw new Error("ZODBASE_TEST_WORKERS must be a positive integer");
let child: ReturnType<typeof Bun.spawn> | undefined;
let signal: "SIGINT" | "SIGTERM" | undefined;
const interrupt = (received: "SIGINT" | "SIGTERM") => {
  signal = received;
  child?.kill(received);
};
const onInterrupt = () => interrupt("SIGINT");
const onTerminate = () => interrupt("SIGTERM");
process.on("SIGINT", onInterrupt);
process.on("SIGTERM", onTerminate);
try {
  await acquireTestDatabaseContainers();
  if (!signal) {
    child = Bun.spawn(
      [
        process.execPath,
        "test",
        ...(compiled ? ["--preload", "zod/compile"] : []),
        ...(workers === "1" ? [] : [`--parallel=${workers}`]),
        "--timeout",
        "40000",
        ...args.filter((arg) => arg !== "--compiled"),
      ],
      {
        cwd: `${import.meta.dir}/..`,
        env: { ...process.env, ZODBASE_TEST_DATABASES: getSharedTestDatabases() },
        stdout: "inherit",
        stderr: "inherit",
        stdin: "inherit",
      },
    );
    process.exitCode = await child.exited;
  }
} finally {
  await releaseTestDatabaseContainers();
  console.info(`Test run including containers: ${((performance.now() - startedAt) / 1000).toFixed(2)}s`);
  process.off("SIGINT", onInterrupt);
  process.off("SIGTERM", onTerminate);
  if (signal) process.exitCode = signal === "SIGINT" ? 130 : 143;
}
