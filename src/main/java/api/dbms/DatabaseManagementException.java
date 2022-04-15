package api.dbms;

public class DatabaseManagementException extends RuntimeException {
    public DatabaseManagementException() {
        super();
    }

    public DatabaseManagementException(String message) {
        super(message);
    }

    public DatabaseManagementException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseManagementException(Throwable cause) {
        super(cause);
    }

    public static DatabaseManagementException wrap(Throwable toWrap) {
        if (toWrap instanceof DatabaseManagementException) {
            return (DatabaseManagementException) toWrap;
        }
        return new DatabaseManagementException(toWrap.getMessage(), toWrap);
    }
}
