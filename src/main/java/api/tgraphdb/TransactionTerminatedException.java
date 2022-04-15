package api.tgraphdb;

public class TransactionTerminatedException extends TransactionFailureException {
    public TransactionTerminatedException(String msg) {
        super(msg);
    }

    public TransactionTerminatedException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
