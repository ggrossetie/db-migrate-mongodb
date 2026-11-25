import { MongoClient, ObjectId } from 'mongodb'
import Base from 'db-migrate-base'

let log;
let type;

const MongodbDriver = Base.extend({

  init: function (connection, internals, mongoString) {
    this._super(internals);
    this.connection = connection;
    this.connectionString = mongoString;
    this._database = this.connection.options.dbName
  },

  /**
   * Runs a write operation, unless db-migrate was started with --dry-run.
   * In dry-run mode the operation is logged and skipped, and the result is undefined.
   *
   * @param description - A short description of the operation (logged in dry-run mode)
   * @param operation   - A function that performs the write and returns a promise
   * @param callback
   */
  _write: function (description, operation, callback) {
    let promise
    if (this.internals.dryRun) {
      log.info(`dry run, skipping ${description}`)
      promise = Promise.resolve()
    } else {
      promise = new Promise(resolve => resolve(operation()))
    }
    if (typeof callback !== 'function') {
      return promise
    }

    promise
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Creates the migrations collection
   *
   * @param callback
   */
  _createMigrationsCollection: function (callback) {
    const promise = this._createList(this.internals.migrationTable)
    if (typeof callback !== 'function') {
      return promise
    }

    promise
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Capabilities read by db-migrate (>= 1.0).
   * The key/value state below is safe to use for the migration lock, _updateKVC is an atomic compare and swap
   * and run_on is set by the clock of the database.
   */
  _meta: {
    supports: {
      locking: true
    }
  },

  /**
   * Creates a collection holding a list of entries ({ name, run_on }), does nothing when it already exists.
   * Part of the state management of db-migrate (>= 1.0).
   *
   * @param collectionName  - The name of the collection to be created
   */
  _createList: function (collectionName) {
    return this._write(`createCollection("${collectionName}")`, () => this.connection.db(this._database).createCollection(collectionName)
      .then(() => undefined)
      .catch(error => {
        // NamespaceExists
        if (error && error.errorResponse && error.errorResponse.code === 48) {
          return
        }
        throw error
      }))
  },

  /**
   * Returns the entries of a list, most recent first.
   *
   * @param collectionName  - The name of the collection
   */
  _getList: function (collectionName) {
    return this._find(collectionName, {}, { run_on: -1, name: -1 })
  },

  /**
   * Adds an entry to a list.
   *
   * @param collectionName  - The name of the collection
   * @param name            - The name of the entry
   */
  _insertEntry: function (collectionName, name) {
    return this._write(`insert("${collectionName}", "${name}")`, () => this.connection.db(this._database).collection(collectionName)
      .updateOne({ _id: new ObjectId() }, [{ $set: { name: { $literal: name }, run_on: '$$NOW' } }], { upsert: true }))
  },

  /**
   * Removes an entry from a list.
   *
   * @param collectionName  - The name of the collection
   * @param name            - The name of the entry
   */
  _deleteEntry: function (collectionName, name) {
    return this._write(`delete("${collectionName}", "${name}")`, () => this.connection.db(this._database).collection(collectionName).deleteOne({ name }))
  },

  /**
   * Removes the entries (or keys) written before the given date.
   *
   * @param collectionName  - The name of the collection
   * @param expiry          - The date
   */
  _deleteExpired: function (collectionName, expiry) {
    return this._write(`deleteExpired("${collectionName}")`, () => this.connection.db(this._database).collection(collectionName).deleteMany({ run_on: { $lte: new Date(expiry) } }))
  },

  /**
   * Creates a collection holding key/value pairs ({ key, value, run_on }), does nothing when it already exists.
   * Part of the state management of db-migrate (>= 1.0).
   *
   * @param collectionName  - The name of the collection to be created
   */
  _createKV: function (collectionName) {
    // creates the collection implicitly, and does nothing when the index already exists
    return this._write(`createCollection("${collectionName}")`, () => this.connection.db(this._database).collection(collectionName)
      .createIndex({ key: 1 }, { unique: true })
      .then(() => undefined))
  },

  /**
   * Returns a key/value pair, or undefined when the key does not exist.
   *
   * @param collectionName  - The name of the collection
   * @param key             - The key
   */
  _getKV: function (collectionName, key) {
    return this.connection.db(this._database).collection(collectionName)
      .findOne({ key }, { projection: { _id: 0 } })
      .then(result => result ?? undefined)
  },

  /**
   * Inserts a key/value pair, fails when the key already exists.
   *
   * @param collectionName  - The name of the collection
   * @param key             - The key
   * @param value           - The value
   */
  _insertKV: function (collectionName, key, value) {
    // an upsert leaving an existing document untouched, as $$NOW (the clock of the database) is not available on insert
    return this._write(`insert("${collectionName}", "${key}")`, () => this.connection.db(this._database).collection(collectionName)
      .updateOne(
        { key },
        [{ $set: { value: { $ifNull: ['$value', { $literal: value }] }, run_on: { $ifNull: ['$run_on', '$$NOW'] } } }],
        { upsert: true }
      )
      .then(result => {
        if (result.upsertedCount !== 1) {
          throw new Error(`Duplicate key "${key}" in collection "${collectionName}"`)
        }
        return result
      }))
  },

  /**
   * Updates the value of a key.
   *
   * @param collectionName  - The name of the collection
   * @param key             - The key
   * @param value           - The new value
   */
  _updateKV: function (collectionName, key, value) {
    return this._write(`update("${collectionName}", "${key}")`, () => this.connection.db(this._database).collection(collectionName)
      .updateOne({ key }, { $set: { value }, $currentDate: { run_on: true } }))
  },

  /**
   * Updates the value of a key, only if the given field still has the expected value (compare and swap).
   *
   * @param collectionName  - The name of the collection
   * @param key             - The key
   * @param value           - The new value
   * @param field           - The name of the field to compare
   * @param expected        - The expected value of the field
   */
  _updateKVC: function (collectionName, key, value, field, expected) {
    return this._write(`update("${collectionName}", "${key}")`, () => this.connection.db(this._database).collection(collectionName)
      .updateOne({ key, [field]: expected }, { $set: { value }, $currentDate: { run_on: true } }))
  },

  /**
   * Removes a key.
   *
   * @param collectionName  - The name of the collection
   * @param key             - The key
   */
  _deleteKV: function (collectionName, key) {
    return this._write(`delete("${collectionName}", "${key}")`, () => this.connection.db(this._database).collection(collectionName).deleteOne({ key }))
  },

  /**
   * An alias for _createMigrationsCollection
   * @see #_createMigrationsCollection
   */
  createMigrationsTable: function (callback) {
    this._createMigrationsCollection(callback)
  },

  /**
   * Creates the seed collection
   *
   * @param callback
   */
  _createSeedsCollection: function (callback) {
    this.createCollection(this.internals.seedTable, callback)
  },

  /**
   * An alias for _createSeedsCollection
   * @see #_createSeedsCollection
   */
  createSeedsTable: function (callback) {
    this._createSeedsCollection(callback);
  },

  /**
   * Creates a collection
   *
   * @param collectionName  - The name of the collection to be created
   * @param callback
   */
  createCollection: function (collectionName, callback) {
    return this._write(`createCollection("${collectionName}")`, () => this.connection.db(this._database).createCollection(collectionName), callback)
  },

  /**
   * An alias for createCollection
   * @see #createCollection
   */
  createTable: function (collectionName, callback) {
    this.createCollection(collectionName, callback)
  },

  switchDatabase: function (options, callback) {
    if (typeof (options) === 'object') {
      if (typeof (options.database) === 'string') {
        this._database = options.database;
      }
    } else if (typeof (options) === 'string') {
      this._database = options;
    }

    if (typeof callback === 'function') {
      callback(null)
    }
  },

  createDatabase: function (dbName, options, callback) {
    // noop, MongoDB automatically creates databases
    if (typeof options === 'function') {
      callback = options
    }

    if (typeof callback === 'function') {
      callback(null)
    }
  },

  dropDatabase: function (dbName, options, callback) {
    if (typeof options === 'function') {
      callback = options
    }

    return this._write(`dropDatabase("${dbName}")`, () => this.connection.db(dbName).dropDatabase(typeof options === 'object' && options !== null ? options : {}), callback)
  },

  /**
   * Drops a collection
   *
   * @param collectionName  - The name of the collection to be dropped
   * @param callback
   */
  dropCollection: function (collectionName, callback) {
    return this._write(`dropCollection("${collectionName}")`, () => this.connection.db(this._database).dropCollection(collectionName), callback)
  },

  /**
   * An alias for dropCollection
   * @see #dropCollection
   */
  dropTable: function (collectionName, callback) {
    this.dropCollection(collectionName, callback)
  },

  /**
   * Renames a collection
   *
   * @param collectionName    - The name of the existing collection to be renamed
   * @param newCollectionName - The new name of the collection
   * @param callback
   */
  renameCollection: function (collectionName, newCollectionName, callback) {
    return this._write(`renameCollection("${collectionName}", "${newCollectionName}")`, () => this.connection.db(this._database).renameCollection(collectionName, newCollectionName), callback)
  },

  /**
   * An alias for renameCollection
   * @see #renameCollection
   */
  renameTable: function (collectionName, newCollectionName, callback) {
    this.renameCollection(collectionName, newCollectionName, callback)
  },

  /**
   * Adds an index to a collection
   *
   * @param collectionName  - The collection to add the index to
   * @param indexName       - The name of the index to add
   * @param columns         - The columns to add an index on
   * @param unique          - A boolean whether this creates a unique index
   * @param callback
   */
  addIndex: function (collectionName, indexName, columns, unique, callback) {
    const keys = columns.reduce((result, column) => {
      result[column] = 1
      return result
    }, {})
    const options = {
      name: indexName,
      unique: unique
    }

    return this._write(`addIndex("${collectionName}", "${indexName}")`, () => this.connection.db(this._database).collection(collectionName).createIndex(keys, options), callback)
  },

  /**
   * Removes an index from a collection
   *
   * @param collectionName  - The collection to remove the index
   * @param indexName       - The name of the index to remove
   * @param callback
   */
  removeIndex: function (collectionName, indexName, callback) {
    return this._write(`removeIndex("${collectionName}", "${indexName}")`, () => this.connection.db(this._database).collection(collectionName).dropIndex(indexName), callback)
  },

  /**
   * Inserts a record(s) into a collection
   *
   * @param collectionName  - The collection to insert into
   * @param toInsert        - The record(s) to insert
   * @param callback
   */
  insert: function (collectionName, toInsert, callback) {
    return this._write(`insert("${collectionName}")`, () => this.connection.db(this._database).collection(collectionName).insertOne(toInsert), callback)
  },

  /**
   * Inserts a migration record into the migration collection
   *
   * @param name                - The name of the migration being run
   * @param callback
   */
  addMigrationRecord: function (name, callback) {
    return this.insert(this.internals.migrationTable, { name: name, run_on: new Date() }, callback)
  },

  /**
   * Inserts a seeder record into the seeder collection
   *
   * @param name                - The name of the seed being run
   * @param callback
   */
  addSeedRecord: function (name, callback) {
    return this.insert(this.internals.seedTable, { name: name, run_on: new Date() }, callback)
  },

  /**
   * Returns the DB instance so custom updates can be made.
   * NOTE: This method exceptionally does not call close() on the database driver when the promise resolves.
   * So the getDbInstance method caller needs to call .close() on its own after finish working with the database driver.
   *
   * In dry-run mode this method throws, because writes made on the raw database instance cannot be intercepted.
   *
   * @returns {Db} a database instance
   */
  _getDbInstance: function () {
    if (this.internals.dryRun) {
      throw new Error('_getDbInstance() is not available in dry-run mode because the driver cannot intercept operations made on the raw database instance. Check `db.internals.dryRun` in your migration to skip them.')
    }
    return this.connection.db(this._database)
  },

  /**
   * Runs a query
   *
   * @param collectionName  - The collection to query on
   * @param query           - The query to run
   * @param sort            - Optional sort
   * @param callback
   */
  _find: function (collectionName, query, sort, callback) {
    const findQuery = this.connection.db(this._database)
      .collection(collectionName)
      .find(query)
    const promise = typeof sort === 'object'
      ? findQuery.sort(sort).toArray()
      : findQuery.toArray()
    const cb = typeof callback === 'function'
      ? callback
      : callback === undefined && typeof sort === 'function' ? sort : undefined
    if (cb === undefined) {
      return promise
    }

    promise
      .then(result => cb(null, result))
      .catch(error => cb(error, null))
  },

  /**
   * Gets all the collection names in mongo
   *
   * @param callback  - The callback to call with the collection names
   */
  _getCollectionNames: function (callback) {
    const promise = this.connection.db(this._database).collections({ nameOnly: true })
      .then(result => result.map(c => c.collectionName))
    if (typeof callback !== 'function') {
      return promise
    }

    promise
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Gets all the indexes for a specific collection
   *
   * @param collectionName  - The name of the collection to get the indexes for
   * @param callback        - The callback to call with the collection names
   */
  _getIndexes: function (collectionName, callback) {
    const promise = this.connection.db(this._database).collection(collectionName).indexInformation()
    if (typeof callback !== 'function') {
      return promise
    }

    promise
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Not implemented
   */
  _all: function () {
    throw new Error('Method not implemented.')
  },

  /**
   * Queries the migrations collection
   *
   * @param callback
   */
  allLoadedMigrations: function (callback) {
    return this._find(this.internals.migrationTable, {}, { run_on: -1 }, callback)
  },

  /**
   * Queries the seed collection
   *
   * @param callback
   */
  allLoadedSeeds: function (callback) {
    return this._find(this.internals.seedTable, {}, { run_on: -1 }, callback)
  },

  /**
   * Deletes a migration
   *
   * @param migrationName       - The name of the migration to be deleted
   * @param callback
   */
  deleteMigration: function (migrationName, callback) {
    return this._write(`deleteMigration("${migrationName}")`, () => this.connection.db(this._database).collection(this.internals.migrationTable).deleteOne({ name: migrationName }), callback)
  },

  /**
   * Deletes a migration
   *
   * @param migrationName       - The name of the migration to be deleted
   * @param callback
   */
  deleteSeed: function (migrationName, callback) {
    return this._write(`deleteSeed("${migrationName}")`, () => {
      this.connection.connect()
      return this.connection.db(this._database).collection(this.internals.seedTable).deleteOne({ name: migrationName })
    }, callback)
  },

  /**
   * Closes the connection to mongodb
   */
  close: function (callback) {
    const promise = this.connection.close()
    if (typeof callback !== 'function') {
      return promise
    }

    promise
      .then(_ => callback(null))
      .catch(error => callback(error, null))
  },

  buildWhereClause: function () {
    throw new Error('There is no NoSQL implementation yet!')
  },

  update: function () {
    throw new Error('There is no NoSQL implementation yet!')
  }
})

function parseColonString(config, port, length) {

  let result = '';

  for (let i = 0; i < length; ++i) {

    result += config.host[i] + ((config.host[i].indexOf(':') === -1) ?
      ':' + port : '') + ',';
  }

  return result.substring(0, result.length - 1);
}

function parseObjects(config, port, length) {

  let result = '';

  for (let i = 0; i < length; ++i) {

    result += config.host[i].host + ((!config.host[i].port) ?
      ':' + port : ':' + config.host[i].port) + ',';
  }

  return result.substring(0, result.length - 1);
}

/**
 * Gets a connection to mongo
 *
 * @param config    - The config to connect to mongo
 * @param intern    - "internals"
 * @param callback  - The callback to call with a MongodbDriver object
 */
export function connect(config, intern, callback) {
  let port;
  let host;

  const internals = intern;

  log = internals.mod.log;
  type = internals.mod.type;

  // The database is optional, db-migrate removes it from the config to create or drop a database (db:create, db:drop).
  // Without a database, the MongoDB driver uses the one of the connection string or "test".

  if (config.url !== undefined) {
    const db = config.db || new MongoClient(config.url, config.options ?? {})
    if (typeof callback !== 'function') {
      return new MongodbDriver(db, intern, config.url)
    }
    callback(null, new MongodbDriver(db, intern, config.url))
    return
  }

  if (config.port === undefined) {
    port = 27017;
  } else {
    port = config.port;
  }

  config.host = Array.isArray(config.hosts) ? config.hosts : config.host;

  if (config.host === undefined) {

    host = 'localhost' + ':' + port;
  } else if (Array.isArray(config.host)) {

    const length = config.host.length;
    host = '';

    if (typeof (config.host[0]) === 'string')
      host = parseColonString(config, port, length);
    else {
      host = parseObjects(config, port, length);
    }
  } else {
    host = config.host + ':' + port;
  }

  let mongoString = 'mongodb://';

  if (config.user !== undefined && config.password !== undefined) {
    // Ensure user and password can contain special characters like "@" so app doesn't throw an exception when connecting to MongoDB
    config.user = encodeURIComponent(config.user);
    config.password = encodeURIComponent(config.password);

    mongoString += config.user + ':' + config.password + '@';
  }

  mongoString += host + '/' + (config.database ?? '');

  const extraParams = [];
  if (config.ssl) {
    extraParams.push('ssl=true');
  }

  if (config.authSource !== undefined && config.user !== undefined && config.password !== undefined) {
    extraParams.push('authSource=' + config.authSource);
  }

  if (config.replicaSet) {
    extraParams.push('replicaSet=' + config.replicaSet);
  }

  if (extraParams.length > 0) {
    mongoString += '?' + extraParams.join('&');
  }

  const db = config.db || new MongoClient(mongoString, config.options ?? {});
  if (typeof callback !== 'function') {
    return new MongodbDriver(db, intern, mongoString)
  }
  callback(null, new MongodbDriver(db, intern, mongoString))
}


