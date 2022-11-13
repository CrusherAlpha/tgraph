package cn.edu.buaa.act.tgraph.property;

import cn.edu.buaa.act.tgraph.impl.tgraphdb.GraphSpaceID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

// Every write will be in memory until Commit.
// Read will go into underlying store.
// And for Neo4j semantic compatibility, write should not be read until commit,
// even this write is from yours.
public class EdgeTemporalPropertyStoreTest {
    @Test
    void testBase() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-base", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        var edge = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir, false);
        try (var batch = edge.startBatchWrite()) {
            var k1 = new EdgeTemporalPropertyKey(1, 2, "crusher", 1);
            var k2 = new EdgeTemporalPropertyKey(1, 2, "crusher", 4);
            var k3 = new EdgeTemporalPropertyKey(1, 2, "crusher", 9);
            String val = "v";
            assertTrue(batch.put(k1, val + 1));
            assertTrue(batch.put(k2, val + 2));
            assertTrue(batch.put(k3, val + 3));
            // before we commit the batch, we cannot read anything.
            assertNull(edge.get(k1));
            assertNull(edge.get(k2));
            assertNull(edge.get(k3));

            assertTrue(edge.commitBatchWrite(batch, false, true, true));
            assertEquals(val + 1, edge.get(k1));
            assertEquals(val + 2, edge.get(k2));
            assertEquals(val + 3, edge.get(k3));
        }
        edge.stop();
    }

    @Test
    void testGet() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-get", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        EdgeTemporalPropertyStore edge = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        try (var batch = edge.startBatchWrite()) {
            for (long idx = 1; idx < 4; ++idx) {
                for (long t = 1; t < 20; t += 2) {
                    batch.put(new EdgeTemporalPropertyKey(idx, idx, "crusher", t), v + (idx * t));
                }
            }
            assertTrue(edge.commitBatchWrite(batch, false, true, true));
        }
        // normal get
        assertEquals(v + 1, edge.get(new EdgeTemporalPropertyKey(1, 1, "crusher", 2)));
        assertNull(edge.get(new EdgeTemporalPropertyKey(1, 1, "crusher", 0)));
        assertNull(edge.get(new EdgeTemporalPropertyKey(4, 4, "crusher", 1)));

        // multi get
        List<EdgeTemporalPropertyKey> keys = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 2; i < 20; i += 2) {
            keys.add(new EdgeTemporalPropertyKey(1, 1, "crusher", i));
            expected.add(v + (i - 1));
        }

        var real = edge.multiGet(keys);

        assertEquals(expected, real);
        edge.stop();
    }

    @Test
    void testPrefixGet() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-prefix-get", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        EdgeTemporalPropertyStore edge = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        try (var batch = edge.startBatchWrite()) {
            for (long idx = 1; idx < 3; ++idx) {
                for (long t = 0; t < 10; ++t) {
                    batch.put(new EdgeTemporalPropertyKey(idx, idx, "crusher", t), v);
                    batch.put(new EdgeTemporalPropertyKey(idx, idx, "alpha", t), v);
                }
            }
            assertTrue(edge.commitBatchWrite(batch, false, true, true));
        }
        // Prefix{1, "crusher"}
        assertEquals(10, edge.prefixGet(new EdgeTemporalPropertyKeyPrefix(1, 1, "crusher")).size());
        // Prefix{1, "alpha"}
        assertEquals(10, edge.prefixGet(new EdgeTemporalPropertyKeyPrefix(1, 1, "alpha")).size());
        // Prefix{2, "crusher"}
        assertEquals(10, edge.prefixGet(new EdgeTemporalPropertyKeyPrefix(2, 2, "crusher")).size());
        // Prefix{2, "alpha"}
        assertEquals(10, edge.prefixGet(new EdgeTemporalPropertyKeyPrefix(2, 2, "alpha")).size());
        // null
        assertNull(edge.prefixGet(new EdgeTemporalPropertyKeyPrefix(1, 1, "crusher-alpha")));
        assertNull(edge.prefixGet(new EdgeTemporalPropertyKeyPrefix(3, 3, "crusher")));
    }

    @Test
    void testRangeGet() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-range-get", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        EdgeTemporalPropertyStore edge = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        try (var batch = edge.startBatchWrite()) {
            for (long t = 0; t < 20; t += 2) {
                batch.put(new EdgeTemporalPropertyKey(1, 1, "crusher", t), v + t);
            }
            assertTrue(edge.commitBatchWrite(batch, false, true, true));
        }

        var start = new EdgeTemporalPropertyKey(1, 1, "crusher", 1);
        var end = new EdgeTemporalPropertyKey(1, 1, "crusher", 10);


        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i += 2) {
            expected.add(v + i);
        }
        List<String> actual = new ArrayList<>();
        var ret = edge.rangeGet(start, end);
        for (var pr : ret) {
            actual.add((String) pr.second());
        }

        assertEquals(expected, actual);

    }

    @Test
    void testRangePrefixGet() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-range-prefix-get", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        EdgeTemporalPropertyStore edge = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        try (var batch = edge.startBatchWrite()) {
            for (long t = 0; t < 20; ++t) {
                batch.put(new EdgeTemporalPropertyKey(1, 1, "crusher", t), v + t);
            }
            assertTrue(edge.commitBatchWrite(batch, false, true, true));
        }

        var start = new EdgeTemporalPropertyKey(1, 1, "crusher", 5);

        var ret = edge.rangeWithPrefixGet(start);
        assertEquals(15, ret.size());
    }

    @Test
    void testRemove() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "test-graph-remove", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        EdgeTemporalPropertyStore edge = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir, false);
        String v = "v";
        // put [0, 20)
        try (var batch = edge.startBatchWrite()) {
            for (long t = 0; t < 20; ++t) {
                batch.put(new EdgeTemporalPropertyKey(1, 1, "crusher", t), v + t);
            }
            assertTrue(edge.commitBatchWrite(batch, false, true, true));
        }

        assertEquals("v10", edge.get(new EdgeTemporalPropertyKey(1, 1, "crusher", 10)));

        try (var batch = edge.startBatchWrite()) {
            batch.remove(new EdgeTemporalPropertyKey(1, 1, "crusher", 10));
            assertTrue(edge.commitBatchWrite(batch, false, true, true));
        }
        assertEquals("v9", edge.get(new EdgeTemporalPropertyKey(1, 1, "crusher", 10)));

        for (long t = 11; t < 20; ++t) {
            assertEquals(v + t, edge.get(new EdgeTemporalPropertyKey(1, 1, "crusher", t)));
        }

        try (var batch = edge.startBatchWrite()) {
            batch.removeRange(new EdgeTemporalPropertyKey(1, 1, "crusher", 11), new EdgeTemporalPropertyKey(1, 1, "crusher", 20));
            assertTrue(edge.commitBatchWrite(batch, false, true, true));
        }

        for (long t = 11; t < 20; ++t) {
            assertEquals("v9", edge.get(new EdgeTemporalPropertyKey(1, 1, "crusher", t)));
        }

    }
}
