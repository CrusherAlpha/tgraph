package dbms;

public class DatabaseExistsException extends DatabaseManagementException {
    public DatabaseExistsException() {
        super();
    }

    public DatabaseExistsException(String message) {
        super(message);
    }

    public DatabaseExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseExistsException(Throwable cause) {
        super(cause);
    }
}
