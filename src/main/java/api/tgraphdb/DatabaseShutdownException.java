package api.tgraphdb;

public class DatabaseShutdownException extends RuntimeException {
    private static final String MESSAGE = "This database is shutdown.";

    public DatabaseShutdownException() {
        super(MESSAGE);
    }

    public DatabaseShutdownException(Throwable cause) {
        super(MESSAGE, cause);
    }

}
