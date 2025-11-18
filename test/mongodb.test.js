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
      await driver.async.createCollection('event')
      const collectionNames = await driver.async._getCollectionNames()
      assert.deepEqual(collectionNames, ["event"])
    } finally {
      await driver.async.close()
    }
  })

  it('should find a document', async () => {
    const connectionString = container.getConnectionString()
    const driver = getDriver(connectionString)
    try {
      await driver.async.createCollection('documents')
      const emptyResult = await driver.async._find('documents', {name: 'foo'})
      assert.deepEqual(emptyResult, [])
      await driver.async.insert('documents', {name: 'foo'})
      const docs = await driver.async._find('documents', {name: 'foo'})
      assert.equal(docs.length, 1)
      assert.equal(docs[0].name, 'foo')
    } finally {
      await driver.async.close()
    }
  })
})
