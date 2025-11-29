declare module '@ggrossetie/db-migrate-mongodb' {
    function connect(config: any, intern: any, callback: undefined | ((err: Error | null, driver: MongodbDriver) => void)): MongodbDriver | undefined

    export = connect

    type Callback<Result> = (err: Error | null, result: Result | null) => void;
    type MongodbDriver = {
        _createMigrationsCollection: (callback: Callback<any>) => void,
        createMigrationsTable: (callback: Callback<any>) => void,
        _createSeedsCollection: (callback: Callback<any>) => void,
        createSeedsTable: (callback: Callback<any>) => void,
        createCollection: (collectionName: string, callback: Callback<any>) => void,
        createTable: (collectionName: string, callback: Callback<any>) => void,
        switchDatabase: (options: any, callback: Callback<any>) => void,
        createDatabase: (dbName: string, options: any, callback: Callback<any>) => void,
        dropDatabase: (dbName: string, options: any, callback: Callback<any>) => void,
        dropCollection: (collectionName: string, callback: Callback<any>) => void,
        dropTable: (collectionName: string, callback: Callback<any>) => void,
        renameCollection: (collectionName: string, newCollectionName: string, callback: Callback<any>) => void,
        renameTable: (collectionName: string, newCollectionName: string, callback: Callback<any>) => void,
        addIndex: (collectionName: string, indexName: string, columns: any, unique: boolean, callback: Callback<any>) => void,
        removeIndex: (collectionName: string, indexName: string, callback: Callback<any>) => void,
        insert: (collectionName: string, toInsert: any, callback: Callback<any>) => void,
        addMigrationRecord: (name: string, callback: Callback<any>) => void,
        addSeedRecord: (name: string, callback: Callback<any>) => void,
        _getDbInstance: () => void,
        _find: (collectionName: string, query: any, sort: any, callback: Callback<any>) => void,
        _getCollectionNames: (callback: Callback<any>) => void,
        _getIndexes: (collectionName: string, callback: Callback<any>) => void,
        _all: () => void,
        allLoadedMigrations: (callback: Callback<any>) => void,
        allLoadedSeeds: (callback: Callback<any>) => void,
        deleteMigration: (migrationName: string, callback: Callback<any>) => void,
        deleteSeed: (migrationName: string, callback: Callback<any>) => void,
        close: (callback: Callback<any>) => void,
        buildWhereClause: () => void,
        update: () => void,
    }
}
