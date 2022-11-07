package txn;

import com.google.common.base.Preconditions;
import common.Codec;
import common.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import property.EdgeTemporalPropertyKey;
import property.EdgeTemporalPropertyKeyPrefix;
import property.VertexTemporalPropertyKey;
import property.VertexTemporalPropertyKeyPrefix;

import java.io.Serializable;
import java.nio.ByteBuffer;

enum LogEntryEntityType {
    VERTEX, EDGE
}

enum RedoLogType {
    APPEND, DELETE_SINGLE, DELETE_RANGE, DELETE_ALL
}


// TODO(crusher): remove all magic numbers.
public class LogEntry implements Serializable {

    private static final Log log = LogFactory.getLog(LogEntry.class);


    // 0x00 -> vertex
    // 0x01 -> edge
    private final byte entityType;
    // 0x00 -> append
    // 0x01 -> delete single
    // 0x02 -> delete range
    // 0x03 -> delete all
    private final byte redoType;
    private final int keyLength;
    private final int valueLength;
    // in delete range redo type, key is start, value is end
    // in delete all redo type, key is prefix
    private final byte[] key;
    private final byte[] value;
    private static final byte[] placeholder = {0x00};

    private LogEntry(byte entity, byte redo, byte[] key, byte[] value) {
        this.entityType = entity;
        this.redoType = redo;
        this.keyLength = key.length;
        this.valueLength = value.length;
        this.key = key;
        this.value = value;
    }

    public LogEntryEntityType entityType() {
        Preconditions.checkState(entityType == 0x00 || entityType == 0x01);
        return entityType == 0x00 ? LogEntryEntityType.VERTEX : LogEntryEntityType.EDGE;
    }

    public RedoLogType redoLogType() {
        Preconditions.checkState(redoType == 0x00 || redoType == 0x01 || redoType == 0x02 || redoType == 0x03);
        switch (redoType) {
            case 0x00: {
                return RedoLogType.APPEND;
            }
            case 0x01: {
                return  RedoLogType.DELETE_SINGLE;
            }
            case 0x02: {
                return RedoLogType.DELETE_RANGE;
            }
            case 0x03: {
                return RedoLogType.DELETE_ALL;
            }
        }
        log.error("unknown redo log type.");
        return null;
    }

    public static LogEntry putVertex(VertexTemporalPropertyKey key, Object value) {
        return new LogEntry((byte) 0x00, (byte) 0x00, key.toBytes(), Codec.encodeValue(value));

    }

    public static LogEntry removeVertex(VertexTemporalPropertyKey key) {
        return new LogEntry((byte) 0x00, (byte) 0x01, key.toBytes(), placeholder);
    }

    public static LogEntry putEdge(EdgeTemporalPropertyKey key, Object value) {
        return new LogEntry((byte) 0x01, (byte) 0x00, key.toBytes(), Codec.encodeValue(value));
    }

    public static LogEntry removeEdge(EdgeTemporalPropertyKey key) {
        return new LogEntry((byte) 0x01, (byte) 0x01, key.toBytes(), placeholder);
    }

    public static LogEntry removeVertexRange(VertexTemporalPropertyKey start, VertexTemporalPropertyKey end) {
        return new LogEntry((byte) 0x00, (byte) 0x02, start.toBytes(), end.toBytes());
    }

    public static LogEntry removeEdgeRange(EdgeTemporalPropertyKey start, EdgeTemporalPropertyKey end) {
        return new LogEntry((byte) 0x01, (byte) 0x02, start.toBytes(), end.toBytes());
    }

    public static LogEntry removeVertexPrefix(VertexTemporalPropertyKeyPrefix prefix) {
        return new LogEntry((byte) 0x00, (byte) 0x03, prefix.toBytes(), placeholder);
    }

    public static LogEntry removeEdgePrefix(EdgeTemporalPropertyKeyPrefix prefix) {
        return new LogEntry((byte) 0x01, (byte) 0x03, prefix.toBytes(), placeholder);
    }

    private static LogEntry doFromBytes(byte[] bytes) {
        // keyLength
        var buffer = ByteBuffer.wrap(bytes, 2, 4);
        var keyLength = buffer.getInt();
        // valueLength
        buffer = ByteBuffer.wrap(bytes, 6, 4);
        var valueLength = buffer.getInt();
        // key
        buffer = ByteBuffer.wrap(bytes, 10, keyLength);
        byte[] key = new byte[keyLength];
        buffer.get(key);
        // value
        buffer = ByteBuffer.wrap(bytes, 10 + keyLength, valueLength);
        byte[] value = new byte[valueLength];
        buffer.get(value);
        return new LogEntry(bytes[0], bytes[1], key, value);
    }

    public static LogEntry fromBytes(byte[] bytes) {
        Preconditions.checkState(bytes.length > 10, "LogEntry data corruption.");
        if (bytes[0] != 0x00) {
            Preconditions.checkState(bytes[0] == 0x01, "LogEntry entity type should be VERTEX or EDGE.");
        }
        if (bytes[1] != 0x00) {
            Preconditions.checkState(bytes[1] == 0x01, "LogEntry redo log type should be APPEND or DELETE.");
        }
        return doFromBytes(bytes);
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 1 + 4 + 4 + key.length + value.length);
        buffer.put(entityType);
        buffer.put(redoType);
        buffer.putInt(keyLength);
        buffer.putInt(valueLength);
        buffer.put(key);
        buffer.put(value);
        return buffer.array();
    }

    public Pair<VertexTemporalPropertyKey, Object> toVertex() {
        Preconditions.checkState(this.entityType == 0x00, "LogEntry entity type should be VERTEX");
        Object val = redoType == 0x00 ? Codec.decodeValue(value) : placeholder;
        return Pair.of(VertexTemporalPropertyKey.fromBytes(key), val);
    }

    public Pair<VertexTemporalPropertyKey, VertexTemporalPropertyKey> toVertexRange() {
        Preconditions.checkState(this.entityType == 0x00, "LogEntry entity type should be VERTEX");
        Preconditions.checkState(this.redoType == 0x02, "LogEntry redo log type should be DELETE_RANGE");
        return Pair.of(VertexTemporalPropertyKey.fromBytes(key), VertexTemporalPropertyKey.fromBytes(value));
    }

    public VertexTemporalPropertyKeyPrefix toVertexPrefix() {
        Preconditions.checkState(this.entityType == 0x00, "LogEntry entity type should be VERTEX");
        Preconditions.checkState(this.redoType == 0x03, "LogEntry redo log type should be DELETE_ALL");
        return VertexTemporalPropertyKeyPrefix.fromBytes(key);
    }

    public Pair<EdgeTemporalPropertyKey, Object> toEdge() {
        Preconditions.checkState(this.entityType == 0x01, "LogEntry entity type should be EDGE");
        Object val = redoType == 0x00 ? Codec.decodeValue(value) : placeholder;
        return Pair.of(EdgeTemporalPropertyKey.fromBytes(key), val);
    }

    public Pair<EdgeTemporalPropertyKey, EdgeTemporalPropertyKey> toEdgeRange() {
        Preconditions.checkState(this.entityType == 0x01, "LogEntry entity type should be EDGE");
        Preconditions.checkState(this.redoType == 0x02, "LogEntry redo log type should be DELETE_RANGE");
        return Pair.of(EdgeTemporalPropertyKey.fromBytes(key), EdgeTemporalPropertyKey.fromBytes(value));
    }

    public EdgeTemporalPropertyKeyPrefix toEdgePrefix() {
        Preconditions.checkState(this.entityType == 0x01, "LogEntry entity type should be EDGE");
        Preconditions.checkState(this.redoType == 0x03, "LogEntry redo log type should be DELETE_ALL");
        return EdgeTemporalPropertyKeyPrefix.fromBytes(key);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "entityType=" + entityType +
                ", redoType=" + redoType +
                ", keyLength=" + keyLength +
                ", valueLength=" + valueLength +
                '}';
    }
}
