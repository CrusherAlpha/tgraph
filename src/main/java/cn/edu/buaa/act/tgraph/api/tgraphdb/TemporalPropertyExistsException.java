package cn.edu.buaa.act.tgraph.api.tgraphdb;

public class TemporalPropertyExistsException extends RuntimeException {
    public TemporalPropertyExistsException() {
        super();
    }

    public TemporalPropertyExistsException(String message) {
        super(message);
    }

    public TemporalPropertyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public TemporalPropertyExistsException(Throwable cause) {
        super(cause);
    }

}
