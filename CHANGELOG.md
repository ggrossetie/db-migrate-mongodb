# Changelog

## 2.0.0 (unreleased)

### Breaking Changes

* Require Node.js >= 22
* `mongodb` is now a peer dependency (`>= 5.1.0 < 8`) — you must install it yourself
* Rewrite as ESM (a CJS bundle is still shipped via `exports`)
* Remove `bluebird` — all methods return native promises

### Features

* Support MongoDB driver 5.x, 6.x, and 7.x
* Test against MongoDB server 6.0, 7.0, and 8.0
* Ship both ESM and CJS via the `exports` field in `package.json`
* All methods return a promise when no callback is provided

### Bug Fixes

* Fix `_createMigrationsCollection` to not fail when called multiple times
* Fix `find`/`insert` methods to use the MongoDB driver API directly
* Fix `_getDbInstance` export (renamed to `_getDbInstance` to avoid being filtered)
* Fix `_find` to disambiguate query and sort options
* Fix `dropIndex` to call the method on the `Collection` object instead of `Db`
* Fix `addIndex` to pass index specification and options correctly to `createIndex`
* Fix `deleteSeed` to call `connect()` before `deleteOne`

---

## 1.5.0

### Bug Fixes

* Allow special characters in password or user string

### Chores

* Drop unused moment library

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.4.0...v1.5.0)

## 1.4.0 (2018-01-08)

### Features

* **getDbInstance:** Added getDbInstance method ([cfe208c](https://github.com/db-migrate/mongodb/commit/cfe208c))

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.3.0...v1.4.0)

## 1.3.0 (2017-10-25)

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.2.3...v1.3.0)

## 1.2.3 (2017-08-14)

### Bug Fixes

* **switchdb:** switching db caused issue due to non async function ([b426ce3](https://github.com/db-migrate/mongodb/commit/b426ce3))

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.2.2...v1.2.3)

## 1.2.2 (2017-06-25)

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.2.1...v1.2.2)

## 1.2.1 (2017-01-13)

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.2.0...v1.2.1)

## 1.2.0 (2017-01-12)

### Features

* **tests:** move tests into the repo of the driver ([5cbabce](https://github.com/db-migrate/mongodb/commit/5cbabce))

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.1.4...v1.2.0)

## 1.1.4 (2016-02-05)

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.1.3...v1.1.4)

## 1.1.3 (2016-02-03)

### Bug Fixes

* **upstream:** bump upstream package to fix dropTable bug ([9841177](https://github.com/db-migrate/mongodb/commit/9841177))

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.1.2...v1.1.3)

## 1.1.2 (2016-01-27)

### Bug Fixes

* **api:** drop table overwrite options on promises ([ed7774a](https://github.com/db-migrate/mongodb/commit/ed7774a))

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.1.1...v1.1.2)

## 1.1.1 (2016-01-12)

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.1.0...v1.1.1)

## 1.1.0 (2016-01-07)

### Features

* **configuration:** Add possibility to specify a host array ([bc6fea2](https://github.com/db-migrate/mongodb/commit/bc6fea2))

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.0.2...v1.1.0)

## 1.0.2 (2015-12-27)

[Changelog](https://github.com/db-migrate/mongodb/compare/v1.0.1...v1.0.2)

## 1.0.1 (2015-09-08)
