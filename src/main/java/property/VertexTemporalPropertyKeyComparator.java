package property;

import common.ComparatorUtil;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

import java.nio.ByteBuffer;

// This comparator assumes keys are VertexTemporalPropertyKey
// nodeId(long, 64-bit) - timestamp(long, 64-bit) - propertyName(String)
// Caller must guarantee that in accessing other APIs in combination with this comparator.
public final class VertexTemporalPropertyKeyComparator extends AbstractComparator {

    public VertexTemporalPropertyKeyComparator(ComparatorOptions comparatorOptions) {
        super(comparatorOptions);
    }

    @Override
    public String name() {
        return "buaa.act.tgraph.VertexTemporalPropertyKeyComparator";
    }


    @Override
    public int compare(ByteBuffer lhs, ByteBuffer rhs) {
        return ComparatorUtil.vertexTemporalPropertyKeyCompare(lhs, rhs);
    }
}
