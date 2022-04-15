package api.tgraphdb;

/**
 * This exception will be thrown when one or less entities were expected,
 * yet multiple were found.
 */
public class MultipleFoundException extends RuntimeException {
    public MultipleFoundException() {
        super();
    }

    public MultipleFoundException(String message) {
        super(message);
    }

    public MultipleFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public MultipleFoundException(Throwable cause) {
        super(cause);
    }
}
