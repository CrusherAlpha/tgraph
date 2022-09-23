package property;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

// For graph structure locality, we identify an edge by start node id + end node id.
// In this way, all edges start from the same node will be stored in the same block.
public class EdgeTemporalPropertyKey {
    private final long startNodeId;
    private final long endNodeId;
    private final String propertyName;
    private final long timestamp;

    public EdgeTemporalPropertyKey(long startNodeId, long endNodeId, String propertyName, long timestamp) {
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.propertyName = propertyName;
        this.timestamp = timestamp;
    }

    public static EdgeTemporalPropertyKey fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, 8);
        long sId = buffer.getLong();
        buffer = ByteBuffer.wrap(bytes, 8, 8);
        long eId = buffer.getLong();
        buffer = ByteBuffer.wrap(bytes, bytes.length - 8, 8);
        long time = buffer.getLong();
        buffer = ByteBuffer.wrap(bytes, 16, bytes.length - 24);
        String property = new String(buffer.array());
        return new EdgeTemporalPropertyKey(sId, eId, property, time);
    }

    public long getStartNodeId() {
        return startNodeId;
    }

    public long getEndNodeId() {
        return endNodeId;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public long getTimestamp() {
        return timestamp;
    }


    public byte[] toBytes() {
        byte[] p = propertyName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(24 + p.length);
        buffer.putLong(startNodeId);
        buffer.putLong(endNodeId);
        buffer.put(p);
        buffer.putLong(timestamp);
        return buffer.array();
    }

}
