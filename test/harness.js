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
  return connect({
    url: connectionString,
    database: "db_migrate_test",
    options: {
      directConnection: true
    }
  }, internals)
}
