package cn.edu.buaa.act.tgraph.property;

import cn.edu.buaa.act.tgraph.common.ComparatorUtil;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

import java.nio.ByteBuffer;

// This comparator assumes keys are EdgeTemporalPropertyKey
// startNodeId(long, 64-bit) - endNodeId(long, 64-bit) - propertyName(String) - timestamp(long, 64-bit)
// Caller must guarantee that in accessing other APIs in combination with this comparator.
public final class EdgeTemporalPropertyKeyComparator extends AbstractComparator {
    public EdgeTemporalPropertyKeyComparator(ComparatorOptions comparatorOptions) {
        super(comparatorOptions);
    }

    @Override
    public String name() {
        return "buaa.act.tgraph.EdgeTemporalPropertyKeyComparator";
    }

    @Override
    public int compare(ByteBuffer lhs, ByteBuffer rhs) {
        return ComparatorUtil.edgeTemporalPropertyKeyCompare(lhs, rhs);
    }
}
