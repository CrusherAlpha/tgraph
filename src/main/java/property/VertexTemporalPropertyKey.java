package property;

public class VertexTemporalPropertyKey {
    private long nodeId;
    private long timestamp;
    private String propertyName;

    public VertexTemporalPropertyKey(long nodeId, long timestamp, String propertyName) {
        this.nodeId = nodeId;
        this.timestamp = timestamp;
        this.propertyName = propertyName;
    }

    public long getNodeId() {
        return nodeId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getPropertyName() {
        return propertyName;
    }
}
