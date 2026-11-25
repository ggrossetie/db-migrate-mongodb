[![CI](https://github.com/ggrossetie/db-migrate-mongodb/actions/workflows/ci.yml/badge.svg)](https://github.com/ggrossetie/db-migrate-mongodb/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@ggrossetie/db-migrate-mongodb)](https://www.npmjs.com/package/@ggrossetie/db-migrate-mongodb)

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
- **Replaced the internal `_run()` dispatcher** — methods call the MongoDB driver API directly. Write operations only go through a thin `_write()` helper, which logs and skips them in [dry-run mode](#dry-run).

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
    "driver": {
      "require": "@ggrossetie/db-migrate-mongodb"
    },
    "database": "my_database",
    "url": {
      "value": "mongodb://localhost:27017/my_database"
    }
  }
}
```

Both object forms matter with db-migrate 0.11.x and 1.x.

- A string `driver` is prefixed with `db-migrate-`, so a scoped package name cannot be resolved. The `require` form loads the package as is.
- A string `url` is parsed by db-migrate, which then replaces `driver` with the URL protocol (`mongodb`) and removes `url`. The `value` form passes the connection string to this driver untouched.

You can also replace `url` with `host`, `port`, `user` and `password`.

```json
{
  "defaultEnv": "dev",
  "dev": {
    "driver": {
      "require": "@ggrossetie/db-migrate-mongodb"
    },
    "database": "my_database",
    "host": "localhost",
    "port": 27017
  }
}
```

The following options are supported when `url` is not set.

| Option           | Description                                                                                  |
|------------------|----------------------------------------------------------------------------------------------|
| `host`           | Host name, or a list of hosts (`"host:port"` strings or `{ "host", "port" }` objects). Defaults to `localhost` |
| `hosts`          | Alias of `host` for a list of hosts                                                          |
| `port`           | Port used when a host does not define one. Defaults to `27017`                               |
| `user`, `password` | Credentials, special characters are encoded for you                                        |
| `authSource`     | Database used to authenticate (only applied with `user` and `password`)                      |
| `replicaSet`     | Name of the replica set                                                                      |
| `ssl`            | Set to `true` to enable TLS                                                                  |

In both forms, `options` is passed as is to the [`MongoClient`](https://mongodb.github.io/node-mongodb-native/) constructor.

The `database` is optional. Without it, the MongoDB driver falls back to the database of the connection string, or `test`.

### Dry run

With `db-migrate up --dry-run`, the driver logs and skips every write operation (`createCollection`, `insert`, `addIndex`...), and these methods resolve to `undefined`.
Read operations still run against the database.

The driver cannot intercept operations made on the raw `Db` instance, so `_getDbInstance()` throws in dry-run mode.
A migration that needs the raw instance can check the flag itself.

```js
exports.up = async function (db) {
  if (db.internals.dryRun) {
    return
  }
  await db._getDbInstance().collection('users').updateMany({}, { $set: { active: true } })
}
```

## Compatibility with db-migrate

The driver is tested against db-migrate 0.11 and 1.9.

Since version 1.0, db-migrate keeps a state next to the migrations and locks the database while migrating, so concurrent processes do not run the same migrations twice.
The driver supports both. The state is stored in the `migrations_state` collection (see the `--state-table` option of db-migrate), which is created on the first run.
Upgrading db-migrate from 0.11 does not require any change, the migrations already recorded in the `migrations` collection are kept.

Migrations must use `_meta.version: 1` (the default). The version 2 format lets db-migrate learn a relational schema (tables, columns, foreign keys), which does not apply to MongoDB.

## Contributing

Contributions are welcome! Read the [contributing guide](CONTRIBUTING.md) to learn how to run the tests, write commit messages, update the changelog and how releases are published.

The notable changes of each version are listed in the [changelog](CHANGELOG.md).

## License

MIT
