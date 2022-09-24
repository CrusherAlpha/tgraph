package property;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;

public class EdgeTemporalPropertyKeyPrefix {
    private final long startNodeId;
    private final long endNodeId;
    private final int propertyLength;
    private final String propertyName;

    public EdgeTemporalPropertyKeyPrefix(long startNodeId, long endNodeId, String propertyName) {
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.propertyName = propertyName;
        this.propertyLength = propertyName.getBytes(Charset.defaultCharset()).length;
    }

    public static EdgeTemporalPropertyKeyPrefix fromBytes(byte[] bytes) {
        Preconditions.checkState(bytes.length > 20);
        // sid
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, 8);
        long sId = buffer.getLong();
        // eid
        buffer = ByteBuffer.wrap(bytes, 8, 8);
        long eId = buffer.getLong();
        // property length
        buffer = ByteBuffer.wrap(bytes, 16, 4);
        int length = buffer.getInt();
        // property name
        buffer = ByteBuffer.wrap(bytes, 20, length);
        byte[] pBytes = new byte[length];
        buffer.get(pBytes);
        String property = new String(pBytes);
        return new EdgeTemporalPropertyKeyPrefix(sId, eId, property);
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

    public byte[] toBytes() {
        byte[] p = propertyName.getBytes(Charset.defaultCharset());
        ByteBuffer buffer = ByteBuffer.allocate(20 + p.length);
        buffer.putLong(startNodeId);
        buffer.putLong(endNodeId);
        buffer.putInt(propertyLength);
        buffer.put(p);
        return buffer.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeTemporalPropertyKeyPrefix that = (EdgeTemporalPropertyKeyPrefix) o;
        return startNodeId == that.startNodeId && endNodeId == that.endNodeId && Objects.equals(propertyName, that.propertyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startNodeId, endNodeId, propertyName);
    }

    // for debug
    @Override
    public String toString() {
        return "EdgeTemporalPropertyKeyPrefix{" +
                "startNodeId=" + startNodeId +
                ", endNodeId=" + endNodeId +
                ", propertyName='" + propertyName + '\'' +
                '}';
    }
}
