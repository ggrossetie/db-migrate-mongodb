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

    it('should throw when getting the raw database instance', () => {
      assert.throws(() => dryRunDriver._getDbInstance(), /not available in dry-run mode/)
    })
  })
  })
}
