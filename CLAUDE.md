# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MongoDB driver plugin for [db-migrate](https://github.com/db-migrate/node-db-migrate). Published as `@ggrossetie/db-migrate-mongodb` on npm. Requires Node.js >= 22.

## Commands

- **Tests:** `npm test` (uses `node --test`, requires Docker for testcontainers/MongoDB)
- **Build:** `npm run build` (rollup bundles `index.js` into `bundle.cjs` for CJS consumers)
- **Run a single test:** `node --test --test-name-pattern="<pattern>" test/mongodb.test.js`

## Architecture

Single-file driver (`index.js`) exporting a `connect()` function. The driver extends `db-migrate-base` and wraps the MongoDB Node.js driver (`mongodb` package).

- **`index.js`** — All driver logic: `MongodbDriver` class (via `Base.extend()`) and `connect()` factory. Every method supports both callback and promise patterns (returns a promise when no callback is provided).
- **`rollup.config.js`** — Builds a CJS bundle (`bundle.cjs`) with `mongodb`, `db-migrate-base`, and `bluebird` as externals.
- **`test/harness.js`** — Test helper that creates a driver instance from a connection string.
- **`test/mongodb.test.js`** — Integration tests using `@testcontainers/mongodb` (spins up a real MongoDB container).

## Key Patterns

- The module is ESM (`"type": "module"`) but also ships a CJS bundle via the `exports` field in `package.json`.
- `mongodb` is a **peerDependency** (>= 5.1.0 < 8), not a direct dependency. The devDependency pins a specific version for testing.
- `MongodbDriver` stores a `MongoClient` instance as `this.connection` and the database name as `this._database`. All DB operations go through `this.connection.db(this._database)`.