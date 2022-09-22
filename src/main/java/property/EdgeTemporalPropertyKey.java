package property;

// For graph structure locality, we identify an edge by start node id + end node id.
// In this way, all edges start from the same node will be stored in the same block.
public class EdgeTemporalPropertyKey {
    private long startNodeId;
    private long endNodeId;
    private long timestamp;
    private String propertyName;

    public EdgeTemporalPropertyKey(long startNodeId, long endNodeId, long timestamp, String propertyName) {
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.timestamp = timestamp;
        this.propertyName = propertyName;
    }

    public long getStartNodeId() {
        return startNodeId;
    }

    public long getEndNodeId() {
        return endNodeId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getPropertyName() {
        return propertyName;
    }
}
