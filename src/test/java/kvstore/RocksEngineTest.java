package kvstore;

import common.Pair;
import impl.tgraphdb.GraphSpaceID;
import org.junit.jupiter.api.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static common.Codec.*;
import static org.junit.jupiter.api.Assertions.*;


public class RocksEngineTest {
    private static final Log log = LogFactory.getLog(RocksEngineTest.class);
    @Test
    void testBase() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-base", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        StoreOptions opt = StoreOptions.of(graph0, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
        String k1 = "test-k1";
        assertArrayEquals(null, kv.get(encodeValue(k1), null));
        String v1 = "test-v1";
        assertTrue(kv.put(encodeValue(k1), encodeValue(v1)));
        assertArrayEquals(encodeValue(v1), kv.get(encodeValue(k1), null));
    }

    @Test
    void testSnapshot() {
        GraphSpaceID graph1 = new GraphSpaceID(2, "test-graph-snapshot", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph1.getGraphName();
        StoreOptions opt = StoreOptions.of(graph1, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
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
        GraphSpaceID graph2 = new GraphSpaceID(3, "test-graph-wb", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph2.getGraphName();
        StoreOptions opt = StoreOptions.of(graph2, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
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

    // only used for this ut.
    private static byte[] intToBytes(final int num) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(num);
        return buffer.array();
    }

    // only used for this ut.
    private static int bytesToInt(final byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }


    @Test
    void testRangeIter() {
        GraphSpaceID graph3 = new GraphSpaceID(4, "test-graph-iter-range", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph3.getGraphName();
        StoreOptions opt = StoreOptions.of(graph3, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
        String v = "v";
        // put [0, 20)
        for (int i = 0; i < 20; ++i) {
            assertTrue(kv.put(intToBytes(i), encodeValue(v + i)));
        }
        // read [0, 10)
        try (var iter = kv.range(intToBytes(0), intToBytes(10))) {
            int ind = 0;
            while (iter.valid()) {
                log.info(String.format("key is %s.", bytesToInt(iter.key())));
                assertArrayEquals(iter.key(), intToBytes(ind));
                assertArrayEquals(iter.value(), encodeValue(v + ind));
                iter.next();
                ++ind;
            }
            assertEquals(10, ind);
        }
    }

    @Test
    void testRangePrevIter() {
        GraphSpaceID graph3 = new GraphSpaceID(4, "test-graph-iter-range-prev", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph3.getGraphName();
        StoreOptions opt = StoreOptions.of(graph3, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
        String v = "v";

        kv.put(intToBytes(1), encodeValue(v + 1));
        kv.put(intToBytes(4), encodeValue(v + 4));
        kv.put(intToBytes(9), encodeValue(v + 9));

        List<Pair<byte[], byte[]>> expected = new ArrayList<>();
        expected.add(Pair.of(intToBytes(1), encodeValue(v + 1)));
        expected.add(Pair.of(intToBytes(4), encodeValue(v + 4)));

        int ord = 0;

        try (var iter = kv.rangePrev(intToBytes(2), intToBytes(5))) {
            while (iter.valid()) {
                assertArrayEquals(expected.get(ord).first(), iter.key());
                assertArrayEquals(expected.get(ord).second(), iter.value());
                iter.next();
                ++ord;
            }
            assertEquals(2, ord);
        }
    }

    // only used for this ut.
    private static byte[] encodeString(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    // only used for this ut.
    private static String decodeString(byte[] bytes) {
        return new String(bytes);
    }

    @Test
    void testPrefixIter() {
        GraphSpaceID graph3 = new GraphSpaceID(4, "test-graph-iter-prefix", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph3.getGraphName();
        StoreOptions opt = StoreOptions.of(graph3, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
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


    // only used in this ut
    private static byte[] rangePrefixKey(String prefix, int ord) {
        ByteBuffer buffer = ByteBuffer.allocate(prefix.getBytes(StandardCharsets.UTF_8).length + 4);
        buffer.put(prefix.getBytes(StandardCharsets.UTF_8));
        buffer.putInt(ord);
        return buffer.array();
    }
    @Test
    void testRangePrefixIter() {
        GraphSpaceID graph3 = new GraphSpaceID(4, "test-graph-iter-range-prefix", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph3.getGraphName();
        StoreOptions opt = StoreOptions.of(graph3, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
        String prefix = "crusher-";
        String v = "v";
        // put [0, 20)
        for (int i = 0; i < 20; ++i) {
            assertTrue(kv.put(rangePrefixKey(prefix, i), encodeValue(v + i)));
        }
        // range prefix start from crusher-10;
        try (var iter = kv.rangeWithPrefix(rangePrefixKey(prefix, 10), encodeString(prefix))) {
            int ind = 10;
            while (iter.valid()) {
                assertArrayEquals(rangePrefixKey(prefix, ind), iter.key());
                assertArrayEquals(encodeValue(v + ind), iter.value());
                iter.next();
                ++ind;
            }
            assertEquals(20, ind);
        }
    }

    @Test
    void testRangePrevPrefixIter() {
        GraphSpaceID graph3 = new GraphSpaceID(4, "test-graph-iter-range-prev-prefix", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph3.getGraphName();
        StoreOptions opt = StoreOptions.of(graph3, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
        String prefix = "crusher-";
        String v = "v";
        // put even in [0, 20)
        for (int i = 0; i < 20; i += 2) {
            assertTrue(kv.put(rangePrefixKey(prefix, i), encodeValue(v + i)));
        }
        // range prefix start from crusher-11;
        try (var iter = kv.rangePrevWithPrefix(rangePrefixKey(prefix, 11), encodeString(prefix))) {
            int ind = 10;
            while (iter.valid()) {
                assertArrayEquals(rangePrefixKey(prefix, ind), iter.key());
                assertArrayEquals(encodeValue(v + ind), iter.value());
                iter.next();
                ind += 2;
            }
            assertEquals(20, ind);
        }
    }

    @Test
    void testGetForPrev() {
        GraphSpaceID graph4 = new GraphSpaceID(5, "test-graph-get-for-prev", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph4.getGraphName();
        StoreOptions opt = StoreOptions.of(graph4, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
        String v = "v";
        // put even [0, 20)
        for (int i = 0; i < 20; i += 2) {
            assertTrue(kv.put(intToBytes(i), encodeValue(v + i)));
        }
        // get odd in [0, 20)
        for (int i = 1; i < 20; i += 2) {
            assertArrayEquals(encodeValue(v + (i - 1)), kv.getForPrev(intToBytes(i), null).second());
        }
    }

    @Test
    void testMultiGetForPrev() {
        GraphSpaceID graph4 = new GraphSpaceID(5, "test-graph-multi-get-for-prev", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph4.getGraphName();
        StoreOptions opt = StoreOptions.of(graph4, baseDir + dataDir, false, null);
        KVEngine kv = new RocksEngine(opt);
        String v = "v";
        // put even [0, 20)
        for (int i = 0; i < 20; i += 2) {
            assertTrue(kv.put(intToBytes(i), encodeValue(v + i)));
        }
        List<byte[]> keys = new ArrayList<>();
        // get odd in [0, 20)
        for (int i = 1; i < 20; i += 2) {
            keys.add(intToBytes(i));
        }
        var ret = kv.multiGetForPrev(keys);

        assertNotNull(ret);

        int ord = 0;

        for (var val : ret) {
            assertArrayEquals(encodeValue(v + ord), val.second());
            ord += 2;
        }

    }
}
