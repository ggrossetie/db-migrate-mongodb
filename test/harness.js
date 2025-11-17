import { promisify } from 'node:util'

import dbMigrateShared from 'db-migrate-shared'
import { connect } from '@ggrossetie/db-migrate-mongodb'

const {dataType, log} = dbMigrateShared

const internals = {
  mod: {
    log: log,
    type: dataType
  },
  interfaces: {
    SeederInterface: {},
    MigratorInterface: {},

  },
  migrationTable: "migrations"
}

export function getDriver(connectionString) {
  const driver = connect({
    url: connectionString,
    database: "db_migrate_test",
    options: {
      directConnection: true
    }
  }, internals)

  async function _call(driver, functionName, args) {
    const promisifyFun = promisify(driver[functionName]).bind(driver)
    return await promisifyFun(...args)
  }

  driver['async'] = {
    createCollection: async (name) => await _call(driver, 'createCollection', [name]),
    _getCollectionNames: async () => await _call(driver, '_getCollectionNames', []),
    close: async () => await _call(driver, 'close', []),
  }

  return driver
}
