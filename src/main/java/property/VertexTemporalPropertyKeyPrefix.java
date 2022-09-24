package property;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;

public class VertexTemporalPropertyKeyPrefix {
    private final long nodeId;
    private final int propertyLength;
    private final String propertyName;

    public VertexTemporalPropertyKeyPrefix(long nodeId, String propertyName) {
        this.nodeId = nodeId;
        this.propertyName = propertyName;
        this.propertyLength = propertyName.getBytes(Charset.defaultCharset()).length;
    }

    public static VertexTemporalPropertyKeyPrefix fromBytes(byte[] bytes) {
        Preconditions.checkState(bytes.length > 12);
        // node id
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, 8);
        long id = buffer.getLong();
        // property length
        buffer = ByteBuffer.wrap(bytes, 8, 4);
        int length = buffer.getInt();
        // property name
        buffer = ByteBuffer.wrap(bytes, 12, length);
        byte[] pBytes = new byte[length];
        buffer.get(pBytes);
        String property = new String(pBytes);
        return new VertexTemporalPropertyKeyPrefix(id, property);
    }

    public long getNodeId() {
        return nodeId;
    }

    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VertexTemporalPropertyKeyPrefix that = (VertexTemporalPropertyKeyPrefix) o;
        return nodeId == that.nodeId && Objects.equals(propertyName, that.propertyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, propertyName);
    }

    public byte[] toBytes() {
        byte[] p = propertyName.getBytes(Charset.defaultCharset());
        ByteBuffer buffer = ByteBuffer.allocate(12 + p.length);
        buffer.putLong(nodeId);
        buffer.putInt(propertyLength);
        buffer.put(p);
        return buffer.array();
    }

    // for debug
    @Override
    public String toString() {
        return "VertexTemporalPropertyKeyPrefix{" +
                "nodeId=" + nodeId +
                ", propertyName='" + propertyName + '\'' +
                '}';
    }
}
