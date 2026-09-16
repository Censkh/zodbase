import type DatabaseAdaptor from "../../DatabaseAdaptor";
import type { DatabaseAdaptorOptions } from "../../DatabaseAdaptor";
import PostgresAdaptor from "../postgres";

/** Pass the function returned by neon(connectionString). No driver dependency is bundled. */
export interface NeonHttpDriver {
  query(text: string, params: any[], options: { arrayMode: false; fullResults: true }): PromiseLike<{ rows: any[] }>;
}

export default class NeonHttpAdaptor extends PostgresAdaptor {
  constructor(options: DatabaseAdaptorOptions<NeonHttpDriver>) {
    super({
      ...options,
      driver: { query: (text) => options.driver.query(text, [], { arrayMode: false, fullResults: true }) },
    });
  }

  override async transaction<TResult>(_callback: (adaptor: DatabaseAdaptor) => Promise<TResult>): Promise<TResult> {
    throw new Error(
      "Neon HTTP does not support interactive transactions; use PostgresAdaptor with Neon's Pool or Client",
    );
  }
}
