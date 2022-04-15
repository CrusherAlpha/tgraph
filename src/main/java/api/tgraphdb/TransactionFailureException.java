package api.tgraphdb;

/**
 * Signals that a transaction failed and has been rolled back.
 */
public class TransactionFailureException extends RuntimeException {
    public TransactionFailureException(String msg) {
        super(msg);
    }

    public TransactionFailureException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
