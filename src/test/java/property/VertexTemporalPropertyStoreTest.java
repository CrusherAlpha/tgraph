package property;

import impl.tgraphdb.GraphSpaceID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class VertexTemporalPropertyStoreTest {

    private static final Log log = LogFactory.getLog(VertexTemporalPropertyStoreTest.class);

    @Test
    void testBase() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-base");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        VertexTemporalPropertyStore vertex = new VertexTemporalPropertyStore(graph0, baseDir + dataDir, false);
        try (var batch = vertex.startBatchWrite()) {
            var k1 = new VertexTemporalPropertyKey(1, "crusher", 1);
            var k2 = new VertexTemporalPropertyKey(1, "crusher", 4);
            var k3 = new VertexTemporalPropertyKey(1, "crusher", 9);
            String val = "v";
            assertTrue(batch.put(k1, val + 1));
            assertTrue(batch.put(k2, val + 2));
            assertTrue(batch.put(k3, val + 3));
            // before we commit the batch, we cannot read anything.
            assertNull(vertex.get(k1));
            assertNull(vertex.get(k2));
            assertNull(vertex.get(k3));

            assertTrue(vertex.commitBatchWrite(batch, false, true, true));
            assertEquals(val + 1, vertex.get(k1));
            assertEquals(val + 2, vertex.get(k2));
            assertEquals(val + 3, vertex.get(k3));
        }
        vertex.stop();
    }

    @Test
    void testGet() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-get");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        VertexTemporalPropertyStore vertex = new VertexTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        try (var batch = vertex.startBatchWrite()) {
            for (long idx = 1; idx < 4; ++idx) {
                for (long t = 1; t < 20; t += 2) {
                    batch.put(new VertexTemporalPropertyKey(idx, "crusher", t), v + (idx * t));
                }
            }
            assertTrue(vertex.commitBatchWrite(batch, false, true, true));
        }
        // normal get
        assertEquals(v + 1, vertex.get(new VertexTemporalPropertyKey(1, "crusher", 2)));
        assertNull(vertex.get(new VertexTemporalPropertyKey(1, "crusher", 0)));
        assertNull(vertex.get(new VertexTemporalPropertyKey(4, "crusher", 1)));

        // multi get
        List<VertexTemporalPropertyKey> keys = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 2; i < 20; i += 2) {
            keys.add(new VertexTemporalPropertyKey(1, "crusher", i));
            expected.add(v + (i - 1));
        }

        var real = vertex.multiGet(keys);

        assertEquals(expected, real);
        vertex.stop();
    }

    @Test
    void testPrefixGet() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-prefix-get");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        VertexTemporalPropertyStore vertex = new VertexTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        try (var batch = vertex.startBatchWrite()) {
            for (long idx = 1; idx < 3; ++idx) {
                for (long t = 0; t < 10; ++t) {
                    batch.put(new VertexTemporalPropertyKey(idx, "crusher", t), v);
                    batch.put(new VertexTemporalPropertyKey(idx, "alpha", t), v);
                }
            }
            assertTrue(vertex.commitBatchWrite(batch, false, true, true));
        }
        // Prefix{1, "crusher"}
        assertEquals(10, vertex.prefixGet(new VertexTemporalPropertyKeyPrefix(1, "crusher")).size());
        // Prefix{1, "alpha"}
        assertEquals(10, vertex.prefixGet(new VertexTemporalPropertyKeyPrefix(1, "alpha")).size());
        // Prefix{2, "crusher"}
        assertEquals(10, vertex.prefixGet(new VertexTemporalPropertyKeyPrefix(2, "crusher")).size());
        // Prefix{2, "alpha"}
        assertEquals(10, vertex.prefixGet(new VertexTemporalPropertyKeyPrefix(2, "alpha")).size());
        // null
        assertNull(vertex.prefixGet(new VertexTemporalPropertyKeyPrefix(1, "crusher-alpha")));
        assertNull(vertex.prefixGet(new VertexTemporalPropertyKeyPrefix(3, "crusher")));
    }

    @Test
    void testRangeGet() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-range-get");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        VertexTemporalPropertyStore vertex = new VertexTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        try (var batch = vertex.startBatchWrite()) {
            for (long t = 0; t < 20; t += 2) {
                batch.put(new VertexTemporalPropertyKey(1, "crusher", t), v + t);
            }
            assertTrue(vertex.commitBatchWrite(batch, false, true, true));
        }

        var start = new VertexTemporalPropertyKey(1, "crusher", 1);
        var end = new VertexTemporalPropertyKey(1, "crusher", 10);


        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i += 2) {
            expected.add(v + i);
        }
        List<String> actual = new ArrayList<>();
        var ret = vertex.rangeGet(start, end);
        for (var pr : ret) {
            actual.add((String) pr.second());
        }

        assertEquals(expected, actual);

    }

    @Test
    void testRangePrefixGet() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-range-prefix-get");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        VertexTemporalPropertyStore vertex = new VertexTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        try (var batch = vertex.startBatchWrite()) {
            for (long t = 0; t < 20; ++t) {
                batch.put(new VertexTemporalPropertyKey(1, "crusher", t), v + t);
            }
            assertTrue(vertex.commitBatchWrite(batch, false, true, true));
        }

        var start = new VertexTemporalPropertyKey(1, "crusher", 5);

        var ret = vertex.rangeWithPrefixGet(start);
        assertEquals(15, ret.size());
    }

    @Test
    void testRemove() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-remove");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        VertexTemporalPropertyStore vertex = new VertexTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        // put [0, 20)
        try (var batch = vertex.startBatchWrite()) {
            for (long t = 0; t < 20; ++t) {
                batch.put(new VertexTemporalPropertyKey(1, "crusher", t), v + t);
            }
            assertTrue(vertex.commitBatchWrite(batch, false, true, true));
        }

        assertEquals("v10", vertex.get(new VertexTemporalPropertyKey(1, "crusher", 10)));

        try (var batch = vertex.startBatchWrite()) {
            batch.remove(new VertexTemporalPropertyKey(1, "crusher", 10));
            assertTrue(vertex.commitBatchWrite(batch, false, true, true));
        }
        assertEquals("v9", vertex.get(new VertexTemporalPropertyKey(1, "crusher", 10)));

        for (long t = 11; t < 20; ++t) {
            assertEquals(v + t, vertex.get(new VertexTemporalPropertyKey(1, "crusher", t)));
        }

        try (var batch = vertex.startBatchWrite()) {
            batch.removeRange(new VertexTemporalPropertyKey(1, "crusher", 11), new VertexTemporalPropertyKey(1, "crusher", 20));
            assertTrue(vertex.commitBatchWrite(batch, false, true, true));
        }

        for (long t = 11; t < 20; ++t) {
            assertEquals("v9", vertex.get(new VertexTemporalPropertyKey(1, "crusher", t)));
        }

    }
}
