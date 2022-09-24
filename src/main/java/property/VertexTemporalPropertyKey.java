package property;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;

public class VertexTemporalPropertyKey {
    private final long nodeId;
    private final int propertyLength;
    private final String propertyName;
    private final long timestamp;

    private static final Log log = LogFactory.getLog(VertexTemporalPropertyKey.class);

    public VertexTemporalPropertyKey(long nodeId, String propertyName, long timestamp) {
        this.nodeId = nodeId;
        this.propertyName = propertyName;
        this.propertyLength = propertyName.getBytes(Charset.defaultCharset()).length;
        this.timestamp = timestamp;
    }

    public VertexTemporalPropertyKey(VertexTemporalPropertyKeyPrefix prefix, long timestamp) {
        this(prefix.getNodeId(), prefix.getPropertyName(), timestamp);
    }

    public static VertexTemporalPropertyKey fromBytes(byte[] bytes) {
        Preconditions.checkState(bytes.length > 20);
        var prefix = VertexTemporalPropertyKeyPrefix.fromBytes(bytes);
        // timestamp
        var buffer = ByteBuffer.wrap(bytes, bytes.length - 8, 8);
        long time = buffer.getLong();
        return new VertexTemporalPropertyKey(prefix, time);
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

    public VertexTemporalPropertyKeyPrefix getPrefix() {
        return new VertexTemporalPropertyKeyPrefix(this.nodeId, this.propertyName);
    }

    public byte[] toBytes() {
        byte[] p = propertyName.getBytes(Charset.defaultCharset());
        ByteBuffer buffer = ByteBuffer.allocate(20 + p.length);
        buffer.putLong(nodeId);
        buffer.putInt(propertyLength);
        buffer.put(p);
        buffer.putLong(timestamp);
        return buffer.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexTemporalPropertyKey that = (VertexTemporalPropertyKey) o;
        return nodeId == that.nodeId && timestamp == that.timestamp && Objects.equals(propertyName, that.propertyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, propertyName, timestamp);
    }

    // for debug
    @Override
    public String toString() {
        return "VertexTemporalPropertyKey{" +
                "nodeId=" + nodeId +
                ", propertyName='" + propertyName + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
