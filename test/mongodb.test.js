import { after, before, describe, it } from 'node:test'
import assert from 'node:assert'

import { MongoDBContainer } from '@testcontainers/mongodb'

import { getDriver } from './harness.js'

const mongoVersions = ['6.0', '7.0', '8.0']

for (const version of mongoVersions) {
  describe(`mongodb ${version}`, function () {

    let container
    before(async () => {
      container = await new MongoDBContainer(`mongo:${version}`).start()
    })

    after(async () => {
      container.stop()
    })

    it('should get a driver', () => {
      const connectionString = container.getConnectionString()
      const driver = getDriver(connectionString)
      assert.notEqual(driver, null)
    })

    it('should create collection', async () => {
      const connectionString = container.getConnectionString()
      const driver = getDriver(connectionString)
      try {
        await driver.createCollection('event')
        const collectionNames = await driver._getCollectionNames()
        assert.deepEqual(collectionNames, ["event"])
      } finally {
        await driver._getDbInstance().dropCollection('event')
      await driver.close()
      }
    })

    it('should find a document', async () => {
      const connectionString = container.getConnectionString()
      const driver = getDriver(connectionString)
      try {
        await driver.createCollection('documents')
        const emptyResult = await driver._find('documents', {name: 'foo'})
        assert.deepEqual(emptyResult, [])
        await driver.insert('documents', {name: 'foo'})
        const docs = await driver._find('documents', {name: 'foo'})
        assert.equal(docs.length, 1)
        assert.equal(docs[0].name, 'foo')
      } finally {
        await driver._getDbInstance().dropCollection('documents')
      await driver.close()
      }
    })

  it('should not fail when calling _createMigrationsCollection multiple times', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      await driver._createMigrationsCollection()
      await driver._createMigrationsCollection()
    } catch (error) {
      assert.fail(`should not fail when calling _createMigrationsCollection multiple times but caught an error: ${error}`)
    } finally {
      await driver.close()
    }
  })

  it('should list existing migrations', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      await driver.addMigrationRecord('/foo')
      const result = await driver.allLoadedMigrations()
      assert.equal(result.length, 1)
      assert.equal(result[0].name, '/foo')
    } finally {
      await driver._getDbInstance().dropCollection(driver.internals.migrationTable)
      await driver.close()
    }
  })

  it('should drop an existing index', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      await driver.createCollection('products')
      const collectionNames = await driver._getCollectionNames()
      assert.deepEqual(collectionNames, ["products"])
      const mongo = driver._getDbInstance()
      const indexesBefore = await mongo .collection('products').listIndexes().toArray()
      assert.deepEqual(indexesBefore.map(k => k.name), ['_id_'])
      await mongo
        .collection('products')
        .createIndex({ updatedAt: -1 }, { unique: false })
      const indexesAfterCreate = await mongo .collection('products').listIndexes().toArray()
      assert.deepEqual(indexesAfterCreate.map(k => k.name), ['_id_', 'updatedAt_-1'])
      await driver.removeIndex('products', 'updatedAt_-1')
      const indexesAfterRemove = await mongo .collection('products').listIndexes().toArray()
      assert.deepEqual(indexesAfterRemove.map(k => k.name), ['_id_'])
      } finally {
      await driver._getDbInstance().dropCollection('products')
      await driver.close()
    }
  })

  it('should add an index', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      await driver.createCollection('users')
      const mongo = driver._getDbInstance()
      const indexesBefore = await mongo .collection('users').listIndexes().toArray()
      assert.deepEqual(indexesBefore.map(k => k.name), ['_id_'])
      await driver.addIndex('users', 'email_1', ['email'], true)
      const indexesAfterCreate = await mongo .collection('users').listIndexes().toArray()
      assert.deepEqual(indexesAfterCreate.map(k => k.name), ['_id_', 'email_1'])
    } finally {
      await driver._getDbInstance().dropCollection('users')
      await driver.close()
    }
  })

  it('should drop a database', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      const mongo = driver.connection.db('db_migrate_test_drop')
      await mongo.createCollection('events')
      assert.deepEqual((await mongo.collections()).map(c => c.collectionName), ['events'])
      await driver.dropDatabase('db_migrate_test_drop')
      assert.deepEqual(await mongo.collections(), [])
      await mongo.createCollection('events')
      await new Promise((resolve, reject) => {
        driver.dropDatabase('db_migrate_test_drop', (error) => error ? reject(error) : resolve())
      })
      assert.deepEqual(await mongo.collections(), [])
    } finally {
      await driver.close()
    }
  })

  describe('dry run', () => {
    let driver
    let dryRunDriver
    before(() => {
      const connectionString = container.getConnectionString()
      driver = getDriver(connectionString)
      dryRunDriver = getDriver(connectionString, { dryRun: true })
    })

    after(async () => {
      await driver._getDbInstance().dropDatabase()
      await driver.close()
      await dryRunDriver.close()
    })

    it('should not create, rename or drop a collection', async () => {
      assert.equal(await dryRunDriver.createCollection('dry_run'), undefined)
      assert.deepEqual(await driver._getCollectionNames(), [])
      await driver.createCollection('dry_run')
      await dryRunDriver.renameCollection('dry_run', 'renamed')
      await dryRunDriver.dropCollection('dry_run')
      assert.deepEqual(await driver._getCollectionNames(), ['dry_run'])
    })

    it('should not insert a document', async () => {
      await dryRunDriver.insert('dry_run', { name: 'foo' })
      assert.deepEqual(await dryRunDriver._find('dry_run', {}), [])
    })

    it('should not add or remove an index', async () => {
      await dryRunDriver.addIndex('dry_run', 'name_1', ['name'], true)
      assert.deepEqual(Object.keys(await driver._getIndexes('dry_run')), ['_id_'])
      await driver.addIndex('dry_run', 'name_1', ['name'], true)
      await dryRunDriver.removeIndex('dry_run', 'name_1')
      assert.deepEqual(Object.keys(await driver._getIndexes('dry_run')), ['_id_', 'name_1'])
    })

    it('should not add or delete a migration or seed record', async () => {
      await dryRunDriver._createMigrationsCollection()
      await dryRunDriver.addMigrationRecord('/dry-run')
      await dryRunDriver.addSeedRecord('/dry-run')
      assert.deepEqual(await dryRunDriver.allLoadedMigrations(), [])
      assert.deepEqual(await dryRunDriver.allLoadedSeeds(), [])
      await driver.addMigrationRecord('/applied')
      await driver.addSeedRecord('/applied')
      await dryRunDriver.deleteMigration('/applied')
      await dryRunDriver.deleteSeed('/applied')
      assert.deepEqual((await driver.allLoadedMigrations()).map(m => m.name), ['/applied'])
      assert.deepEqual((await driver.allLoadedSeeds()).map(m => m.name), ['/applied'])
    })

    it('should not drop a database', async () => {
      await dryRunDriver.dropDatabase('db_migrate_test')
      assert.notDeepEqual(await driver._getCollectionNames(), [])
    })

    it('should support callbacks', async () => {
      const result = await new Promise((resolve, reject) => {
        dryRunDriver.insert('dry_run', { name: 'foo' }, (error, result) => error ? reject(error) : resolve(result))
      })
      assert.equal(result, undefined)
      assert.deepEqual(await driver._find('dry_run', {}), [])
    })

    it('should not write the state of db-migrate', async () => {
      await dryRunDriver._createList('dry_run_list')
      await dryRunDriver._createKV('dry_run_state')
      await dryRunDriver._insertEntry('dry_run_list', 'a')
      await dryRunDriver._insertKV('dry_run_state', 'lock', 'one')
      const names = await driver._getCollectionNames()
      assert.ok(!names.includes('dry_run_list'))
      assert.ok(!names.includes('dry_run_state'))
      await driver._createKV('dry_run_state')
      await driver._insertKV('dry_run_state', 'lock', 'one')
      await dryRunDriver._updateKV('dry_run_state', 'lock', 'two')
      await dryRunDriver._updateKVC('dry_run_state', 'lock', 'two', 'value', 'one')
      await dryRunDriver._deleteKV('dry_run_state', 'lock')
      assert.equal((await dryRunDriver._getKV('dry_run_state', 'lock')).value, 'one')
    })

    it('should throw when getting the raw database instance', () => {
      assert.throws(() => dryRunDriver._getDbInstance(), /not available in dry-run mode/)
    })
  })

  it('should manage a list of entries', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      await driver._createList('entries')
      await driver._createList('entries')
      assert.deepEqual(await driver._getList('entries'), [])
      await driver._insertEntry('entries', 'a')
      await driver._insertEntry('entries', '$b')
      const entries = await driver._getList('entries')
      assert.deepEqual(entries.map(e => e.name).sort(), ['$b', 'a'])
      assert.ok(entries.every(e => e.run_on instanceof Date))
      await driver._deleteEntry('entries', 'a')
      assert.deepEqual((await driver._getList('entries')).map(e => e.name), ['$b'])
      await driver._deleteExpired('entries', new Date(Date.now() + 60_000))
      assert.deepEqual(await driver._getList('entries'), [])
    } finally {
      await driver._getDbInstance().dropCollection('entries')
      await driver.close()
    }
  })

  it('should manage key/value pairs', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      await driver._createKV('state')
      await driver._createKV('state')
      assert.equal(await driver._getKV('state', 'lock'), undefined)

      await driver._insertKV('state', 'lock', '$one')
      const inserted = await driver._getKV('state', 'lock')
      assert.deepEqual(Object.keys(inserted).sort(), ['key', 'run_on', 'value'])
      assert.equal(inserted.key, 'lock')
      assert.equal(inserted.value, '$one')
      assert.ok(inserted.run_on instanceof Date)

      // a key can only be inserted once
      await assert.rejects(driver._insertKV('state', 'lock', 'other'), /Duplicate key "lock"/)
      assert.equal((await driver._getKV('state', 'lock')).value, '$one')

      await driver._updateKV('state', 'lock', 'two')
      assert.equal((await driver._getKV('state', 'lock')).value, 'two')

      // compare and swap, the update only applies when the expected value matches
      await driver._updateKVC('state', 'lock', 'three', 'value', '$one')
      assert.equal((await driver._getKV('state', 'lock')).value, 'two')
      await driver._updateKVC('state', 'lock', 'three', 'value', 'two')
      assert.equal((await driver._getKV('state', 'lock')).value, 'three')

      await driver._deleteKV('state', 'lock')
      assert.equal(await driver._getKV('state', 'lock'), undefined)
    } finally {
      await driver._getDbInstance().dropCollection('state')
      await driver.close()
    }
  })

  it('should let a single concurrent compare and swap win', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      await driver._createKV('state')
      await driver._insertKV('state', 'lock', 'free')
      const candidates = Array.from({ length: 10 }, (_, i) => `owner-${i}`)
      const results = await Promise.all(candidates.map(c => driver._updateKVC('state', 'lock', c, 'value', 'free')))
      assert.equal(results.filter(r => r.modifiedCount === 1).length, 1)
      assert.match((await driver._getKV('state', 'lock')).value, /^owner-\d$/)
    } finally {
      await driver._getDbInstance().dropCollection('state')
      await driver.close()
    }
  })
  })
}
