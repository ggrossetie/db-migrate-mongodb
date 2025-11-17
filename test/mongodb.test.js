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
})
