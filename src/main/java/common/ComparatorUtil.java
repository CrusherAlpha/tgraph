package common;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class ComparatorUtil {
    public static int longCompare(ByteBuffer lhs, ByteBuffer rhs) {
        long lL = lhs.getLong();
        long lR = rhs.getLong();
        if (lL != lR) {
            return lL < lR ? -1 : 1;
        }
        return 0;
    }

    private static int doVertexTemporalPropertyKeyCompare(ByteBuffer lhs, ByteBuffer rhs) {
        int id = longCompare(lhs, rhs);
        if (id == 0) {
            byte[] pL = new byte[lhs.limit() - 8 - lhs.position()];
            lhs.get(pL);
            byte[] pR = new byte[rhs.limit() - 8 - rhs.position()];
            lhs.get(pR);
            int p = BytewiseComparator.INSTANCE.getInstance().compare(pL, pR);
            return p == 0 ? longCompare(lhs, rhs) : p;
        }
        return id;
    }

    public static int vertexTemporalPropertyKeyCompare(ByteBuffer lhs, ByteBuffer rhs) {
        Preconditions.checkState(lhs.limit() - lhs.position() > 16);
        Preconditions.checkState(rhs.limit() - rhs.position() > 16);
        return doVertexTemporalPropertyKeyCompare(lhs, rhs);
    }

    public static int edgeTemporalPropertyKeyCompare(ByteBuffer lhs, ByteBuffer rhs) {
        Preconditions.checkState(lhs.limit() - lhs.position() > 24);
        Preconditions.checkState(rhs.limit() - rhs.position() > 24);
        int sId = longCompare(lhs, rhs);
        return sId == 0 ? doVertexTemporalPropertyKeyCompare(lhs, rhs) : sId;
    }
}
