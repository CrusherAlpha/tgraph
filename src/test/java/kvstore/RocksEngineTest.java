package kvstore;

import impl.tgraphdb.GraphSpaceID;
import org.junit.jupiter.api.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static common.Codec.*;
import static org.junit.jupiter.api.Assertions.*;


public class RocksEngineTest {
    private static final Log log = LogFactory.getLog(RocksEngineTest.class);
    @Test
    void testBase() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-base");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        KVEngine kv = new RocksEngine(graph0, baseDir + dataDir, false);
        String k1 = "test-k1";
        assertArrayEquals(null, kv.get(encodeValue(k1), null));
        String v1 = "test-v1";
        assertTrue(kv.put(encodeValue(k1), encodeValue(v1)));
        assertArrayEquals(encodeValue(v1), kv.get(encodeValue(k1), null));
    }

    @Test
    void testSnapshot() {
        GraphSpaceID graph1 = new GraphSpaceID(2, "test-graph-snapshot");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph1.getGraphName();
        KVEngine kv = new RocksEngine(graph1, baseDir + dataDir, false);
        // put [k0, k5)
        String k = "k";
        String v = "v";
        for (int i = 0; i < 5; ++i) {
            assertTrue(kv.put(encodeValue(k + i), encodeValue(v + i)));
        }
        // take snapshot
        var snapshot = kv.getSnapshot();
        // put [k5, k10)
        for (int i = 5; i < 10; ++i) {
            assertTrue(kv.put(encodeValue(k + i), encodeValue(v + i)));
        }
        // we can not get k5 from snapshot
        assertArrayEquals(null, kv.get(encodeValue("k5"), snapshot));
        assertArrayEquals(encodeValue("v5"), kv.get(encodeValue("k5"), null));
    }

    @Test
    void testWriteBatch() {
        GraphSpaceID graph2 = new GraphSpaceID(3, "test-graph-wb");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph2.getGraphName();
        KVEngine kv = new RocksEngine(graph2, baseDir + dataDir, false);
        String k = "k";
        String v = "v";
        try (var wb = kv.startBatchWrite()) {
            // [k0, k10)
            for (int i = 0; i < 10; ++i) {
                assertTrue(wb.put(encodeValue(k + i), encodeValue(v + i)));
            }
            // before we commit, we can not read these keys
            for (int i = 0; i < 10; ++i) {
                assertNull(kv.get(encodeValue(k + i), null));
            }
            assertTrue(kv.commitBatchWrite(wb, false, true, true));
            // after we commit, we can read these keys
            for (int i = 0; i < 10; ++i) {
                assertArrayEquals(encodeValue(v + i), kv.get(encodeValue(k + i), null));
            }
        }
    }

    // TODO(crusher): figure out the semantics here.
    @Test
    void testRangeIter() {
        GraphSpaceID graph3 = new GraphSpaceID(4, "test-graph-iter-range");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph3.getGraphName();
        KVEngine kv = new RocksEngine(graph3, baseDir + dataDir, false);
        String k = "k";
        String v = "v";
        // put [k0, k20)
        for (int i = 0; i < 20; ++i) {
            assertTrue(kv.put(encodeString(k + i), encodeValue(v + i)));
        }
        // read [k0, k10)
        try (var iter = kv.range(encodeString(k + "0"), encodeString(k + "10"))) {
            int ind = 0;
            while (iter.valid()) {
                log.info(String.format("key is %s.", decodeString(iter.key())));
                assertArrayEquals(iter.key(), encodeString(k + ind));
                assertArrayEquals(iter.value(), encodeValue(v + ind));
                iter.next();
                ++ind;
            }
            assertEquals(10, ind);
        }
    }

    @Test
    void testPrefixIter() {
        GraphSpaceID graph3 = new GraphSpaceID(4, "test-graph-iter-prefix");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph3.getGraphName();
        KVEngine kv = new RocksEngine(graph3, baseDir + dataDir, false);
        String prefix = "crusher-";
        String k = "k";
        String v = "v";
        for (int i = 0; i < 5; ++i) {
            assertTrue(kv.put(encodeString(prefix + k + i), encodeValue(prefix + v + i)));
            assertTrue(kv.put(encodeString(k + i), encodeValue(v + i)));
        }
        try (var iter = kv.prefix(encodeString(prefix), null)) {
            int ind = 0;
            while (iter.valid()) {
                assertArrayEquals(encodeString(prefix + k + ind), iter.key());
                assertArrayEquals(encodeValue(prefix + v + ind), iter.value());
                iter.next();
                ++ind;
            }
            assertEquals(5, ind);
        }
    }

    @Test
    void testRangePrefixIter() {
        GraphSpaceID graph3 = new GraphSpaceID(4, "test-graph-iter-range-prefix");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph3.getGraphName();
        KVEngine kv = new RocksEngine(graph3, baseDir + dataDir, false);
        String prefix = "crusher-";
    }

    @Test
    void testMore() {
        GraphSpaceID graph4 = new GraphSpaceID(5, "test-graph-more");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph4.getGraphName();
        KVEngine kv = new RocksEngine(graph4, baseDir + dataDir, false);
    }
}
