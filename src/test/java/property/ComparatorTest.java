package property;

import impl.tgraphdb.GraphSpaceID;
import kvstore.Comparator;
import kvstore.RocksEngine;
import kvstore.StoreOptions;
import org.junit.jupiter.api.Test;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.util.IntComparator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ComparatorTest {
    static {
        RocksDB.loadLibrary();
    }

    // only used for this ut.
    private static byte[] intToBytes(final int num) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(num);
        return buffer.array();
    }
    @Test
    void testBase() {
        GraphSpaceID graph = new GraphSpaceID(1, "test-comparator-base", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph.getGraphName();
        StoreOptions opt = StoreOptions.of(graph, baseDir + dataDir, false, new Comparator(new IntComparator(new ComparatorOptions())));
        var kv = new RocksEngine(opt);
        var batch = kv.startBatchWrite();
        String v = "v";
        batch.put(intToBytes(1), v.getBytes(StandardCharsets.UTF_8));
        batch.put(intToBytes(2), v.getBytes(StandardCharsets.UTF_8));
        batch.put(intToBytes(3), v.getBytes(StandardCharsets.UTF_8));
        assertTrue(kv.commitBatchWrite(batch, false, true, true));
    }

    @Test
    void testVertex() {
        GraphSpaceID graph = new GraphSpaceID(1, "test-comparator-vertex", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph.getGraphName();
        StoreOptions opt = StoreOptions.of(graph, baseDir + dataDir, false, new Comparator(new VertexTemporalPropertyKeyComparator(new ComparatorOptions())));
        var kv = new RocksEngine(opt);
        var batch = kv.startBatchWrite();
        String v = "v";
        VertexTemporalPropertyKey k1 = new VertexTemporalPropertyKey(1, "crusher", 1);
        VertexTemporalPropertyKey k2 = new VertexTemporalPropertyKey(1, "crusher", 2);
        batch.put(k1.toBytes(), v.getBytes(StandardCharsets.UTF_8));
        batch.put(k2.toBytes(), v.getBytes(StandardCharsets.UTF_8));
        assertTrue(kv.commitBatchWrite(batch, false, true, true));
    }

    @Test
    void testEdge() {
        GraphSpaceID graph = new GraphSpaceID(1, "test-comparator-edge", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph.getGraphName();
        StoreOptions opt = StoreOptions.of(graph, baseDir + dataDir, false, new Comparator(new EdgeTemporalPropertyKeyComparator(new ComparatorOptions())));
        var kv = new RocksEngine(opt);
        var batch = kv.startBatchWrite();
        String v = "v";
        var k1 = new EdgeTemporalPropertyKey(1, 2, "crusher", 1);
        var k2 = new EdgeTemporalPropertyKey(1, 2, "crusher", 2);
        batch.put(k1.toBytes(), v.getBytes(StandardCharsets.UTF_8));
        batch.put(k2.toBytes(), v.getBytes(StandardCharsets.UTF_8));
        assertTrue(kv.commitBatchWrite(batch, false, true, true));
    }
}
