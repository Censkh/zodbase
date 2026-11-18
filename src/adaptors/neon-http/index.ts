import type DatabaseAdaptor from "../../DatabaseAdaptor";
import type { DatabaseAdaptorOptions } from "../../DatabaseAdaptor";
import PostgresAdaptor from "../postgres";

/** Pass the function returned by neon(connectionString). No driver dependency is bundled. */
export interface NeonHttpDriver {
  query(text: string, params: any[], options: { arrayMode: false; fullResults: true }): PromiseLike<{ rows: any[] }>;
}

export default class NeonHttpAdaptor extends PostgresAdaptor {
  constructor(options: DatabaseAdaptorOptions<NeonHttpDriver>) {
    const wrap = (driver: NeonHttpDriver) => ({
      query: (text: string) => driver.query(text, [], { arrayMode: false, fullResults: true }),
    });
    super(
      options.lazy
        ? { ...options, driver: async () => wrap(await options.driver()) }
        : { ...options, driver: wrap(options.driver) },
    );
  }

  /** Neon's HTTP endpoint takes one statement a request: a batch runs in a transaction instead. */
  override async executeTogether(): Promise<undefined> {
    return undefined;
  }

  override async transaction<TResult>(_callback: (adaptor: DatabaseAdaptor) => Promise<TResult>): Promise<TResult> {
    throw new Error(
      "Neon HTTP does not support interactive transactions; use PostgresAdaptor with Neon's Pool or Client",
    );
  }
}
