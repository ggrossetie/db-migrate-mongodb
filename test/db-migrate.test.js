import { after, before, describe, it } from 'node:test'
import assert from 'node:assert'
import { execFile } from 'node:child_process'
import fs from 'node:fs/promises'
import { createRequire } from 'node:module'
import os from 'node:os'
import path from 'node:path'
import { promisify } from 'node:util'

import { MongoClient } from 'mongodb'
import { MongoDBContainer } from '@testcontainers/mongodb'

const require = createRequire(import.meta.url)
const execFileAsync = promisify(execFile)

const migrationsDir = path.join(import.meta.dirname, 'fixtures', 'migrations')
const allMigrations = ['/20250101000000-create-pets', '/20250102000000-create-owners']

// the oldest and the newest supported versions of db-migrate
const packages = ['db-migrate-0.11', 'db-migrate']

describe('upgrade db-migrate from 0.11 to 1.x', () => {
  const database = 'db_migrate_upgrade'
  let container
  let client

  before(async () => {
    container = await new MongoDBContainer('mongo:8.0').start()
    client = new MongoClient(container.getConnectionString(), { directConnection: true })
  })

  after(async () => {
    await client.close()
    await container.stop()
  })

  function getInstance (packageName) {
    const dbmigrate = require(packageName).getInstance(true, {
      cwd: import.meta.dirname,
      env: 'test',
      throwUncatched: true,
      config: {
        test: {
          driver: { require: '@ggrossetie/db-migrate-mongodb' },
          host: container.getHost(),
          port: container.getMappedPort(27017),
          database,
          options: {
            directConnection: true
          }
        }
      },
      cmdOptions: {
        'migrations-dir': migrationsDir
      }
    })
    dbmigrate.silence(true)
    return dbmigrate
  }

  it('should keep the migrations recorded by the previous version', async () => {
    await getInstance('db-migrate-0.11').up()
    const records = await client.db(database).collection('migrations').find().sort({ name: 1 }).toArray()
    assert.deepEqual(records.map(r => r.name), allMigrations)

    // nothing to run, otherwise the unique index on pets would be violated
    await getInstance('db-migrate').up()
    const recordsAfter = await client.db(database).collection('migrations').find().sort({ name: 1 }).toArray()
    assert.deepEqual(recordsAfter, records)

    await getInstance('db-migrate').down(1)
    const remaining = await client.db(database).collection('migrations').find().toArray()
    assert.deepEqual(remaining.map(r => r.name), ['/20250101000000-create-pets'])
    const collections = await client.db(database).listCollections({}, { nameOnly: true }).toArray()
    assert.ok(!collections.map(c => c.name).includes('owners'))
  })
})

for (const packageName of packages) {
  const DBMigrate = require(packageName)
  const { version } = require(`${packageName}/package.json`)
  const bin = path.join(path.dirname(require.resolve(`${packageName}/package.json`)), 'bin', 'db-migrate')
  // the state management (and the migration lock) was introduced in db-migrate 1.0
  const hasState = Number(version.split('.')[0]) >= 1

  describe(`db-migrate ${version}`, function () {
    let container
    let client
    let tmpDir

    before(async () => {
      container = await new MongoDBContainer('mongo:8.0').start()
      client = new MongoClient(container.getConnectionString(), { directConnection: true })
      tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'db-migrate-mongodb-'))
    })

    after(async () => {
      await client.close()
      await container.stop()
      await fs.rm(tmpDir, { recursive: true, force: true })
    })

    function getInstance (database) {
      const dbmigrate = DBMigrate.getInstance(true, {
        cwd: import.meta.dirname,
        env: 'test',
        throwUncatched: true,
        config: {
          test: {
            driver: { require: '@ggrossetie/db-migrate-mongodb' },
            database,
            // an object, otherwise db-migrate parses the URL and replaces the driver
            url: { value: `${container.getConnectionString()}/${database}` },
            options: {
              directConnection: true
            }
          }
        },
        cmdOptions: {
          'migrations-dir': migrationsDir
        }
      })
      dbmigrate.silence(true)
      return dbmigrate
    }

    async function collectionNames (database) {
      const collections = await client.db(database).listCollections({}, { nameOnly: true }).toArray()
      return collections.map(c => c.name).sort()
    }

    async function migrationNames (database) {
      const records = await client.db(database).collection('migrations').find().sort({ name: 1 }).toArray()
      return records.map(r => r.name)
    }

    describe('API', () => {
      const database = 'db_migrate_api'

      it('should run all the migrations up', async () => {
        await getInstance(database).up()
        assert.deepEqual(await migrationNames(database), allMigrations)
        const names = await collectionNames(database)
        assert.ok(names.includes('pets'))
        assert.ok(names.includes('owners'))
        const pets = await client.db(database).collection('pets').find().toArray()
        assert.deepEqual(pets.map(p => p.name), ['Rex'])
        const indexes = await client.db(database).collection('pets').listIndexes().toArray()
        assert.deepEqual(indexes.map(i => i.name), ['_id_', 'name_1'])
      })

      it('should have nothing left to run', async () => {
        await getInstance(database).up()
        assert.deepEqual(await migrationNames(database), allMigrations)
        const pets = await client.db(database).collection('pets').find().toArray()
        assert.equal(pets.length, 1)
      })

      it('should keep the state and release the migration lock', { skip: !hasState }, async () => {
        const state = await client.db(database).collection('migrations_state').find().toArray()
        const keys = state.map(s => s.key)
        assert.ok(keys.includes('__dbmigrate_schema__'))
        const lock = state.find(s => s.key === '__dbmigrate_state__')
        assert.ok(lock, 'the lock should have been written')
        assert.ok(lock.run_on instanceof Date)
        const { s } = JSON.parse(lock.value)
        assert.equal(s.ID, 0, 'the lock should have been released')
        assert.equal(s.fin, 1)
      })

      it('should run the last migration down', async () => {
        await getInstance(database).down(1)
        assert.deepEqual(await migrationNames(database), ['/20250101000000-create-pets'])
        const names = await collectionNames(database)
        assert.ok(names.includes('pets'))
        assert.ok(!names.includes('owners'))
      })

      it('should report the pending migration', async () => {
        const pending = await getInstance(database).check()
        assert.deepEqual(pending.map(file => file.name), ['20250102000000-create-owners'])
      })

      it('should reset all the migrations', async () => {
        await getInstance(database).reset()
        assert.deepEqual(await migrationNames(database), [])
        assert.ok(!(await collectionNames(database)).includes('pets'))
      })
    })

    describe('CLI', () => {
      const database = 'db_migrate_cli'
      let configFile

      before(async () => {
        configFile = path.join(tmpDir, 'database.json')
        await fs.writeFile(configFile, JSON.stringify({
          test: {
            driver: { require: '@ggrossetie/db-migrate-mongodb' },
            host: container.getHost(),
            port: container.getMappedPort(27017),
            database,
            options: {
              directConnection: true
            }
          }
        }))
      })

      function run (...args) {
        return execFileAsync(process.execPath, [
          bin, ...args, '--config', configFile, '--migrations-dir', migrationsDir, '--env', 'test'
        ], { cwd: tmpDir })
      }

      it('should run the migrations up', async () => {
        const { stdout, stderr } = await run('up')
        // db-migrate warns when the driver lacks the state management or the migration lock
        assert.doesNotMatch(stdout + stderr, /\[WARN]/)
        assert.deepEqual(await migrationNames(database), allMigrations)
      })

      it('should run the migrations down', async () => {
        await run('down', '--count', '2')
        assert.deepEqual(await migrationNames(database), [])
        assert.ok(!(await collectionNames(database)).includes('pets'))
      })

      it('should run the migrations once when started concurrently', { skip: !hasState }, async () => {
        await Promise.all([run('up'), run('up'), run('up')])
        assert.deepEqual(await migrationNames(database), allMigrations)
        const pets = await client.db(database).collection('pets').find().toArray()
        assert.equal(pets.length, 1)
      })

      it('should run the migrations down again', async () => {
        await run('down', '--count', '2')
        assert.deepEqual(await migrationNames(database), [])
        assert.ok(!(await collectionNames(database)).includes('pets'))
      })
    })
  })
}
