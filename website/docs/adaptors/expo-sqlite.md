---
title: Expo SQLite
description: Use Expo’s asynchronous SQLite database interface with Zodbase and understand native-runtime limitations.
---

Use this adaptor with Expo SQLite's asynchronous database interface.

## Install and connect

```bash
npm install zodbase zod
npx expo install expo-sqlite
```

```ts
import * as SQLite from "expo-sqlite";
import { Database } from "zodbase";
import ExpoSQLiteAdaptor from "zodbase/adaptors/expo-sqlite";

const driver = await SQLite.openDatabaseAsync("app.db");
const db = new Database({ adaptor: new ExpoSQLiteAdaptor({ driver }) });
```

Initialize the connection once in your application's database setup, rather than opening it on every component render. See the [Expo SQLite reference](https://docs.expo.dev/versions/latest/sdk/sqlite/) for platform setup and driver options.

## Execution and transactions

Reads and row-returning statements use `getAllAsync()`. Other statements use `runAsync()`. `updateMany` runs updates sequentially.

The adaptor inherits explicit SQL transaction handling; it does not wrap Expo's `withTransactionAsync` or `withExclusiveTransactionAsync`. Use `db.transaction()` and its `tx` instance, and avoid unrelated work on the same connection during the callback. Nested transactions are rejected.

## Runtime and lifecycle

Zodbase's browser-condition exports are stubs, not a working database client. Verify your native bundler resolves the implementation rather than those browser entries, and test on the actual device runtime. The package's driver-interface tests do not establish full Expo application compatibility.

The adaptor currently logs failed SQL and the error to `console.error`; account for that when collecting device logs. It inherits [SQLite schema behavior](/adaptors/sqlite/).

When the application no longer needs the database and all work has finished, close it with `await driver.closeAsync()`.
