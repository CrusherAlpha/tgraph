package property;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VertexTemporalPropertyKey {
    private final long nodeId;
    private final String propertyName;
    private final long timestamp;

    public VertexTemporalPropertyKey(long nodeId, String propertyName, long timestamp) {
        this.nodeId = nodeId;
        this.propertyName = propertyName;
        this.timestamp = timestamp;
    }

    public static VertexTemporalPropertyKey fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, 8);
        long id = buffer.getLong();
        buffer = ByteBuffer.wrap(bytes, bytes.length - 8, 8);
        long time = buffer.getLong();
        buffer = ByteBuffer.wrap(bytes, 8, bytes.length - 16);
        String property = new String(buffer.array());
        return new VertexTemporalPropertyKey(id, property, time);
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

    public byte[] toBytes() {
        byte[] p = propertyName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(16 + p.length);
        buffer.putLong(nodeId);
        buffer.put(p);
        buffer.putLong(timestamp);
        return buffer.array();
    }
}
