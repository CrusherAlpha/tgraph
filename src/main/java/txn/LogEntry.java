package txn;

import com.google.common.base.Preconditions;
import common.Codec;
import common.Pair;
import property.EdgeTemporalPropertyKey;
import property.VertexTemporalPropertyKey;

import java.nio.ByteBuffer;

public class LogEntry {
    // 0x00 -> vertex
    // 0x01 -> edge
    private final byte type;
    private final int keyLength;
    private final int valueLength;
    private final byte[] key;
    private final byte[] value;

    private LogEntry(byte type, byte[] key, byte[] value) {
        this.type = type;
        this.keyLength = key.length;
        this.valueLength = value.length;
        this.key = key;
        this.value = value;
    }

    public static LogEntry fromVertex(VertexTemporalPropertyKey key, Object value) {
        return new LogEntry((byte) 0x00, key.toBytes(), Codec.encodeValue(value));
    }

    public static LogEntry fromEdge(EdgeTemporalPropertyKey key, Object value) {
        return new LogEntry((byte) 0x01, key.toBytes(), Codec.encodeValue(value));
    }

    private static LogEntry doFromBytes(byte[] bytes) {
        // keyLength
        var buffer = ByteBuffer.wrap(bytes, 1, 4);
        var keyLength = buffer.getInt();
        // valueLength
        buffer = ByteBuffer.wrap(bytes, 5, 4);
        var valueLength = buffer.getInt();
        // key
        buffer = ByteBuffer.wrap(bytes, 9, keyLength);
        byte[] key = new byte[keyLength];
        buffer.get(key);
        // value
        buffer = ByteBuffer.wrap(bytes, 9 + keyLength, valueLength);
        byte[] value = new byte[valueLength];
        buffer.get(value);
        return new LogEntry(bytes[0], key, value);
    }

    public static LogEntry fromBytes(byte[] bytes) {
        Preconditions.checkState(bytes.length > 9, "LogEntry data corruption.");
        if (bytes[0] != 0x00) {
            Preconditions.checkState(bytes[0] == 0x01, "LogEntry type should be VERTEX or EDGE.");
        }
        return doFromBytes(bytes);
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + key.length + value.length);
        buffer.put(type);
        buffer.putInt(keyLength);
        buffer.putInt(valueLength);
        buffer.put(key);
        buffer.put(value);
        return buffer.array();
    }

    public Pair<VertexTemporalPropertyKey, Object> toVertex() {
        Preconditions.checkState(this.type == 0x00, "LogEntry type should be VERTEX");
        return Pair.of(VertexTemporalPropertyKey.fromBytes(key), Codec.decodeValue(value));
    }

    public Pair<EdgeTemporalPropertyKey, Object> toEdge() {
        Preconditions.checkState(this.type == 0x01, "LogEntry type should be EDGE");
        return Pair.of(EdgeTemporalPropertyKey.fromBytes(key), Codec.decodeValue(value));
    }

}
