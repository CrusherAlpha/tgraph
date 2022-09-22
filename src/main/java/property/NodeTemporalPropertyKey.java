package property;

public class NodeTemporalPropertyKey {
    private long nodeId;
    private long timestamp;
    private String propertyName;

    public NodeTemporalPropertyKey(long nodeId, long timestamp, String propertyName) {
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
