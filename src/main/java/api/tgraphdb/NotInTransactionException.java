package api.tgraphdb;

/**
 * Thrown when attempting to access or modify the graph outside of a transaction.
 *
 * @see Transaction
 */
public class NotInTransactionException extends RuntimeException {
    public NotInTransactionException() {
        super("The requested operation cannot be performed, because it has to be performed in a transaction.");
    }

    public NotInTransactionException(String message) {
        super(message);
    }

    public NotInTransactionException(Throwable cause) {
        super(cause);
    }

    public NotInTransactionException(String message, Throwable cause) {
        super(message, cause);
    }
}
