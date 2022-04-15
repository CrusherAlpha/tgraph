package dbms;

public class DatabaseNotFoundException extends DatabaseManagementException {
    public DatabaseNotFoundException() {
        super();
    }

    public DatabaseNotFoundException(String message) {
        super(message);
    }

    public DatabaseNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseNotFoundException(Throwable cause) {
        super(cause);
    }
}

