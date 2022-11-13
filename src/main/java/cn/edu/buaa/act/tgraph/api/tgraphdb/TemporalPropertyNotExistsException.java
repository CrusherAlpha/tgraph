package cn.edu.buaa.act.tgraph.api.tgraphdb;


public class TemporalPropertyNotExistsException extends RuntimeException {
    public TemporalPropertyNotExistsException() {
        super();
    }

    public TemporalPropertyNotExistsException(String message) {
        super(message);
    }

    public TemporalPropertyNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public TemporalPropertyNotExistsException(Throwable cause) {
        super(cause);
    }

}

