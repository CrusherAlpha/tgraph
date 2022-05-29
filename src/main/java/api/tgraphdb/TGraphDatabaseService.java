package api.tgraphdb;

import java.util.concurrent.TimeUnit;

/**
 * <p>
 * TGraphDatabaseService represents a temporal graph database and is used to create
 * new transactions with beginTx().
 */
public interface TGraphDatabaseService {
    /**
     * Use this method to check if the database is currently in a usable state.
     *
     * @param timeout timeout (in milliseconds) to wait for the database to become available.
     *                If the database has been shut down {@code false} is returned immediately.
     * @return the state of the database: {@code true} if it is available, otherwise {@code false}
     */
    boolean isAvailable(long timeout);

    /**
     * Starts a new transaction and associates it with the current thread.
     * <p>
     * <em>All database operations must be wrapped in a transaction.</em>
     * <p>
     * If you attempt to access the graph outside a transaction, those operations will throw
     * NotInTransactionException.
     * <p>
     * Please ensure that any returned ResourceIterable is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return a new transaction instance
     */
    Transaction beginTx();

    /**
     * Starts a new transaction with custom timeout and associates it with the current thread.
     * Timeout will be taken into account <b>only</b> when execution guard is enabled.
     * <p>
     * <em>All database operations must be wrapped in a transaction.</em>
     * <p>
     * If you attempt to access the graph outside a transaction, those operations will throw
     * NotInTransactionException.
     * <p>
     * Please ensure that any returned ResourceIterable is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param timeout transaction timeout
     * @param unit    time unit of timeout argument
     * @return a new transaction instance
     */
    Transaction beginTx(long timeout, TimeUnit unit);


    /**
     * Return name of underlying database
     *
     * @return database name
     */
    String databaseName();
}
