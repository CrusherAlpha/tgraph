package cn.edu.buaa.act.tgraph.api.dbms;

import cn.edu.buaa.act.tgraph.api.tgraphdb.TGraphDatabaseService;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseNotFoundException;

import java.util.List;

public interface DatabaseManagementService {
    /**
     * Retrieve a database service by name.
     * If database already start, just return, or should start it.
     *
     * @param databaseName name of the database.
     * @return the database service with the provided name
     * @throws DatabaseNotFoundException if no database service with the given name is found.
     */
    TGraphDatabaseService database(String databaseName) throws DatabaseNotFoundException;

    /**
     * Create a new database.
     * Create does not mean start, you should start it first.
     *
     * @param databaseName name of the database.
     * @throws DatabaseExistsException if a database with the provided name already exists
     */
    void createDatabase(String databaseName) throws DatabaseExistsException;

    /**
     * Drop a database by name. All data stored in the database will be deleted as well.
     * You should shut down the database first.
     *
     * @param databaseName name of the database to drop.
     * @throws DatabaseNotFoundException if no database with the given name is found.
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
     * @return A list of all database names this database server manages.
     */
    List<String> listDatabases();

    /**
     * Shutdown database manager.
     */
    void shutdown();
}
