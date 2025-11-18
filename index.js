import { MongoClient } from 'mongodb'
import Base from 'db-migrate-base'
import Promise from 'bluebird'

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
   * Creates the migrations collection
   *
   * @param callback
   */
  _createMigrationsCollection: function (callback) {
    this.createCollection(this.internals.migrationTable, (error, result) => {
      // NamespaceExists
      if (error && error.errorResponse && error.errorResponse.code === 48) {
        // ignore
        callback(null, result)
        return
      }
      callback(error, result)
    })
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
    this.connection.db(this._database).createCollection(collectionName)
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
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
      if (typeof (options.database) === 'string')
        this._database = options.database;
    } else if (typeof (options) === 'string') {
      this._database = options;
    }

    callback(null)
  },

  createDatabase: function (dbName, options, callback) {
    // noop, MongoDB automatically creates databases
    if (typeof options === 'function') {
      callback = options
    }

    callback(null)
  },

  dropDatabase: function (dbName, options, callback) {
    if (typeof options === 'function') {
      callback = options
    }

    this.connection.dropDatabase(dbName, typeof options === 'object' ? options : {})
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Drops a collection
   *
   * @param collectionName  - The name of the collection to be dropped
   * @param callback
   */
  dropCollection: function (collectionName, callback) {
    this.connection.db(this._database).dropCollection(collectionName)
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
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
    this.connection.db(this._database).renameCollection(collectionName, newCollectionName)
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
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
   * @parma callback
   */
  addIndex: function (collectionName, indexName, columns, unique, callback) {
    const options = {
      indexName: indexName,
      columns: columns,
      unique: unique
    }

    this.connection.db(this._database).createIndex(collectionName, options)
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Removes an index from a collection
   *
   * @param collectionName  - The collection to remove the index
   * @param indexName       - The name of the index to remove
   * @param callback
   */
  removeIndex: function (collectionName, indexName, callback) {
    this.connection.db(this._database).dropIndex(collectionName, indexName)
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Inserts a record(s) into a collection
   *
   * @param collectionName  - The collection to insert into
   * @param toInsert        - The record(s) to insert
   * @param callback
   */
  insert: function (collectionName, toInsert, callback) {
    this.connection.db(this._database).collection(collectionName).insertOne(toInsert)
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Inserts a migration record into the migration collection
   *
   * @param name                - The name of the migration being run
   * @param callback
   */
  addMigrationRecord: function (name, callback) {
    this.insert(this.internals.migrationTable, {name: name, run_on: new Date()}, callback)
  },

  /**
   * Inserts a seeder record into the seeder collection
   *
   * @param name                - The name of the seed being run
   * @param callback
   */
  addSeedRecord: function (name, callback) {
    this.insert(this.internals.seedTable, {name: name, run_on: new Date()}, callback)
  },

  /**
   * Returns the DB instance so custom updates can be made.
   * NOTE: This method exceptionally does not call close() on the database driver when the promise resolves. 
   * So the getDbInstance method caller needs to call .close() on its own after finish working with the database driver.
   *
   * @returns {Db} a database instance
   */
  getDbInstance: function () {
    return this.connection.db(this._database)
  },

  /**
   * Runs a query
   *
   * @param collectionName  - The collection to query on
   * @param query           - The query to run
   * @param callback
   */
  _find: function (collectionName, query, callback) {
    this.connection.db(this._database).collection(collectionName).find(query).toArray()
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Gets all the collection names in mongo
   *
   * @param callback  - The callback to call with the collection names
   */
  _getCollectionNames: function (callback) {
    this.connection.db(this._database).collections({nameOnly: true})
      .then(result => callback(null, result.map(c => c.collectionName)))
      .catch(error => callback(error, null))
  },

  /**
   * Gets all the indexes for a specific collection
   *
   * @param collectionName  - The name of the collection to get the indexes for
   * @param callback        - The callback to call with the collection names
   */
  _getIndexes: function (collectionName, callback) {
    this.connection.db(this._database).collection(collectionName).indexInformation()
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Runs a NoSQL command regardless of the dry-run param
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
    this._find(this.internals.migrationTable, {sort: {run_on: -1}}, callback)
  },

  /**
   * Queries the seed collection
   *
   * @param callback
   */
  allLoadedSeeds: function (callback) {
    this._find(this.internals.seedTable, {sort: {run_on: -1}}, callback)
  },

  /**
   * Deletes a migration
   *
   * @param migrationName       - The name of the migration to be deleted
   * @param callback
   */
  deleteMigration: function (migrationName, callback) {
    this.connection.db(this._database).collection(this.internals.migrationTable).deleteOne({name: migrationName})
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Deletes a migration
   *
   * @param migrationName       - The name of the migration to be deleted
   * @param callback
   */
  deleteSeed: function (migrationName, callback) {
    this.connection.db(this._database).collection(this.internals.seedTable).deleteOne({name: migrationName})
      .then(result => callback(null, result))
      .catch(error => callback(error, null))
  },

  /**
   * Closes the connection to mongodb
   */
  close: function (callback) {
    this.connection.close()
      .then(_ => callback(null))
      .catch(error => callback(error, null))
  },

  buildWhereClause: function () {
    throw new Error('There is no NoSQL implementation yet!')
  },

  update: function () {
    throw new Error('There is no NoSQL implementation yet!')
  }
});

Promise.promisifyAll(MongodbDriver);

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

  // Make sure the database is defined
  if (config.database === undefined) {
    throw new Error('database must be defined in database.json');
  }

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

  mongoString += host + '/' + config.database;

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
  

