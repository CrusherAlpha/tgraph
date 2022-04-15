package api.tgraphdb;

/**
 * Thrown when the database is asked to modify data in a way that violates one or more
 * constraints that it is expected to uphold.
 * <p>
 * For instance, if removing a node that still has relationships.
 */
public class ConstraintViolationException extends RuntimeException {
    public ConstraintViolationException(String msg) {
        super(msg);
    }

    public ConstraintViolationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
