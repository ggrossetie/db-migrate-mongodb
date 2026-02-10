[![CI](https://github.com/ggrossetie/db-migrate-mongodb/actions/workflows/ci.yml/badge.svg)](https://github.com/ggrossetie/db-migrate-mongodb/actions/workflows/ci.yml)

# @ggrossetie/db-migrate-mongodb

MongoDB driver for [db-migrate](https://github.com/db-migrate/node-db-migrate).

This is a fork of [db-migrate/mongodb](https://github.com/db-migrate/mongodb), rewritten for modern Node.js.

## Differences with upstream

|                | [db-migrate/mongodb](https://github.com/db-migrate/mongodb) v1.5.0 | @ggrossetie/db-migrate-mongodb v2.x |
|----------------|--------------------------------------------------------------------|-------------------------------------|
| Module system  | CommonJS                                                           | ESM (with CJS bundle)               |
| MongoDB driver | ^3.3.3 (direct dependency)                                         | \>= 5.1.0 < 8 (peer dependency)     |
| Promises       | [bluebird](https://github.com/petkaantonov/bluebird)               | Native                              |
| Node.js        | No requirement                                                     | \>= 22                              |
| Tested against | MongoDB 2.x/3.x                                                    | MongoDB 6.0, 7.0, 8.0               |

### Key changes

- **ESM first** — the source is an ES module. A CJS bundle (`bundle.cjs`) is also shipped for compatibility via the `exports` field in `package.json`.
- **`mongodb` is a peer dependency** — you install the MongoDB driver version you need, instead of being locked to a specific major version.
- **No bluebird** — all methods return native promises (and still accept an optional callback for backward compatibility with db-migrate).
- **Removed the internal `_run()` abstraction** — methods call the MongoDB driver API directly.

## Installation

```bash
npm install @ggrossetie/db-migrate-mongodb mongodb
```

## Usage

Refer to the [db-migrate documentation](https://db-migrate.readthedocs.io/) for general usage.

Configure your `database.json`:

```json
{
  "defaultEnv": "dev",
  "dev": {
    "driver": "@ggrossetie/db-migrate-mongodb",
    "database": "my_database",
    "url": "mongodb://localhost:27017/my_database"
  }
}
```

## License

MIT
