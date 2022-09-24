package common;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import static common.Bytes.memcmp;

public class ComparatorUtil {
    public static int longCompare(ByteBuffer lhs, ByteBuffer rhs) {
        long lL = lhs.getLong();
        long lR = rhs.getLong();
        if (lL != lR) {
            return lL < lR ? -1 : 1;
        }
        return 0;
    }


    private static int doVertexTemporalPropertyKeyCompare(ByteBuffer lhs, ByteBuffer rhs, int start) {
        // compare node id first
        int id = longCompare(lhs, rhs);
        if (id == 0) {
            // compare property
            Preconditions.checkState(lhs.remaining() > 4, "property name should not be empty");
            Preconditions.checkState(rhs.remaining() > 4, "property name should not be empty");
            int lPLen = lhs.getInt();
            int rPLen = rhs.getInt();

            int minLen = Math.min(lPLen, rPLen);

            int r = memcmp(lhs, rhs, start, minLen);

            if (r == 0) {
                if (lPLen < rPLen) {
                    r = -1;
                } else if (rPLen < lPLen) {
                    r = 1;
                }
            }

            if (r == 0) {
                // compare timestamp
                lhs.position(lhs.position() + lPLen);
                rhs.position(rhs.position() + rPLen);
                if (!lhs.hasRemaining() && !rhs.hasRemaining()) {
                    return 0;
                } else if (!lhs.hasRemaining()) {
                    return -1;
                } else if (!rhs.hasRemaining()) {
                    return 1;
                }
                Preconditions.checkState(lhs.remaining() == 8, "timestamp should be 8 bytes.");
                Preconditions.checkState(rhs.remaining() == 8, "timestamp should be 8 bytes.");
                return longCompare(lhs, rhs);
            }
            return r;
        }
        return id;
    }

    public static int vertexTemporalPropertyKeyCompare(ByteBuffer lhs, ByteBuffer rhs) {
        Preconditions.checkNotNull(lhs, "lhs should not be null.");
        Preconditions.checkNotNull(rhs, "rhs should not be null.");
        // VertexTemporalKeyPrefix also will be compared, 12: node id + propertyLength + propertyName(should not be empty)
        Preconditions.checkState(lhs.remaining() > 12, String.format("VertexTemporalPropertyKey should be at least 12 bytes, the real is %d.", lhs.remaining()));
        Preconditions.checkState(rhs.remaining() > 12, String.format("VertexTemporalPropertyKey should be at least 12 bytes, the real is %d.", rhs.remaining()));
        return doVertexTemporalPropertyKeyCompare(lhs, rhs, 12);
    }

    public static int edgeTemporalPropertyKeyCompare(ByteBuffer lhs, ByteBuffer rhs) {
        Preconditions.checkNotNull(lhs, "lhs should not be null.");
        Preconditions.checkNotNull(rhs, "rhs should not be null.");
        // VertexTemporalKeyPrefix also will be compared, 16: start node id + end node id + propertyLength + propertyName(should not be empty)
        Preconditions.checkState(lhs.remaining() > 20, String.format("EdgeTemporalPropertyKey should be at least 20 bytes, the real is %d.", lhs.remaining()));
        Preconditions.checkState(rhs.remaining() > 20, String.format("EdgeTemporalPropertyKey should be at least 20 bytes, the real is %d.", rhs.remaining()));
        // compare start id.
        int sId = longCompare(lhs, rhs);
        return sId == 0 ? doVertexTemporalPropertyKeyCompare(lhs, rhs, 20) : sId;
    }
}
