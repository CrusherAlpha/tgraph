package api.dbms;

import api.tgraphdb.TGraphDatabaseService;

import java.util.List;

public interface DatabaseManagementService {
    /**
     * Retrieve a database service by name.
     *
     * @param databaseName name of the database.
     * @return the database service with the provided name
     * @throws DatabaseNotFoundException if no database service with the given name is found.
     */
    TGraphDatabaseService database(String databaseName) throws DatabaseNotFoundException;

    /**
     * Create a new database.
     *
     * @param databaseName name of the database.
     * @throws DatabaseExistsException if a database with the provided name already exists
     */
    default void createDatabase(String databaseName) throws DatabaseExistsException {
        createDatabase(databaseName, Configuration.EMPTY);
    }

    /**
     * Create a new database.
     *
     * @param databaseName             name of the database.
     * @param databaseSpecificSettings settings that are specific to this database. Only a sub-set of settings are supported TODO.
     * @throws DatabaseExistsException if a database with the provided name already exists
     */
    void createDatabase(String databaseName, Configuration databaseSpecificSettings) throws DatabaseExistsException;

    /**
     * Drop a database by name. All data stored in the database will be deleted as well.
     *
     * @param databaseName name of the database to drop.
     * @throws DatabaseNotFoundException    if no database with the given name is found.
     */
    void dropDatabase(String databaseName) throws DatabaseNotFoundException;

    /**
     * Starts an already existing database.
     *
     * @param databaseName name of the database to start.
     * @throws DatabaseNotFoundException if no database with the given name is found.
     */
    void startDatabase(String databaseName) throws DatabaseNotFoundException;

    /**
     * Shutdown database with provided name.
     *
     * @param databaseName name of the database.
     * @throws DatabaseNotFoundException if no database with the given name is found.
     */
    void shutdownDatabase(String databaseName) throws DatabaseNotFoundException;

    /**
     * @return an alphabetically sorted list of all database names this database server manages.
     */
    List<String> listDatabases();

    /**
     * Shutdown database server.
     */
    void shutdown();
}
