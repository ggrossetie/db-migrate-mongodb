import { after, before, describe, it } from 'node:test'
import assert from 'node:assert'

import { MongoDBContainer } from '@testcontainers/mongodb'

import { getDriver } from './harness.js'

describe('mongodb', function () {

  let container
  before(async () => {
    container = await new MongoDBContainer('mongo:5.0.31').start()
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
})
