package property;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;

// For graph structure locality, we identify an edge by start node id + end node id.
// In this way, all edges start from the same node will be stored in the same block.
public class EdgeTemporalPropertyKey {
    private final long startNodeId;
    private final long endNodeId;
    private final int propertyLength;
    private final String propertyName;
    private final long timestamp;

    public EdgeTemporalPropertyKey(long startNodeId, long endNodeId, String propertyName, long timestamp) {
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.propertyName = propertyName;
        this.propertyLength = propertyName.getBytes(Charset.defaultCharset()).length;
        this.timestamp = timestamp;
    }

    public EdgeTemporalPropertyKey(EdgeTemporalPropertyKeyPrefix prefix, long timestamp) {
        this(prefix.getStartNodeId(), prefix.getEndNodeId(), prefix.getPropertyName(), timestamp);
    }

    public static EdgeTemporalPropertyKey of(long startNodeId, long endNodeId, String propertyName, long timestamp) {
        return new EdgeTemporalPropertyKey(startNodeId, endNodeId, propertyName, timestamp);
    }

    public static EdgeTemporalPropertyKey fromBytes(byte[] bytes) {
        Preconditions.checkState(bytes.length > 28);
        var prefix = EdgeTemporalPropertyKeyPrefix.fromBytes(bytes);
        // timestamp
        ByteBuffer buffer = ByteBuffer.wrap(bytes, bytes.length - 8, 8);
        long time = buffer.getLong();
        return new EdgeTemporalPropertyKey(prefix, time);
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

    public EdgeTemporalPropertyKeyPrefix getPrefix() {
        return new EdgeTemporalPropertyKeyPrefix(this.startNodeId, this.endNodeId, this.propertyName);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeTemporalPropertyKey that = (EdgeTemporalPropertyKey) o;
        return startNodeId == that.startNodeId && endNodeId == that.endNodeId && timestamp == that.timestamp && Objects.equals(propertyName, that.propertyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startNodeId, endNodeId, propertyName, timestamp);
    }

    public byte[] toBytes() {
        byte[] p = propertyName.getBytes(Charset.defaultCharset());
        ByteBuffer buffer = ByteBuffer.allocate(28 + p.length);
        buffer.putLong(startNodeId);
        buffer.putLong(endNodeId);
        buffer.putInt(propertyLength);
        buffer.put(p);
        buffer.putLong(timestamp);
        return buffer.array();
    }

    // for debug
    @Override
    public String toString() {
        return "EdgeTemporalPropertyKey{" +
                "startNodeId=" + startNodeId +
                ", endNodeId=" + endNodeId +
                ", propertyName='" + propertyName + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
