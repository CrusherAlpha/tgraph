package cn.edu.buaa.act.tgraph.txn;

import cn.edu.buaa.act.tgraph.impl.tgraphdb.GraphSpaceID;
import cn.edu.buaa.act.tgraph.property.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LogApplierTest {

    private static final Log log = LogFactory.getLog(LogStoreTest.class);



    private VertexTemporalPropertyKey makeVertex(long nodeId, long timestamp) {
        return new VertexTemporalPropertyKey(nodeId, "test-vp", timestamp);
    }

    private VertexTemporalPropertyKeyPrefix makeVertexPrefix(long nodeId) {
        return VertexTemporalPropertyKeyPrefix.of(nodeId, "test-vp");
    }

    private EdgeTemporalPropertyKey makeEdge(long startId, long endId, long timestamp) {
        return new EdgeTemporalPropertyKey(startId, endId, "test-ep", timestamp);
    }

    private EdgeTemporalPropertyKeyPrefix makeEdgePrefix(long startId, long endId) {
        return EdgeTemporalPropertyKeyPrefix.of(startId, endId, "test-ep");
    }

    @Test
    void testBasic() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "log-applier-test-basic", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        var vs = new VertexTemporalPropertyStore(graph0, baseDir + dataDir + "/vertex", false);
        var es = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir + "/edge", false);
        var ls = new LogStore(graph0, baseDir + dataDir + "/log");

        var applier = new LogApplier(vs, es);

        // copy from LogStoreTest
        List<VertexTemporalPropertyKey> vertex = new ArrayList<>();
        List<EdgeTemporalPropertyKey> edge = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            vertex.add(makeVertex(i, i));
            edge.add(makeEdge(i, i+1, i));
        }

        // before apply, expect empty
        for (int i = 0; i < 10; ++i) {
            assertNull(vs.get(vertex.get(i)));
            assertNull(es.get(edge.get(i)));
        }

        var wb = ls.startBatchWrite();

        int ind = 0;
        for (var v : vertex) {
            wb.append(LogEntry.putVertex(v, "v" + ind));
            ++ind;
        }

        ind = 0;
        for (var e : edge) {
            wb.append(LogEntry.putEdge(e, "e" + ind));
            ++ind;
        }

        ls.commitBatchWrite(1, wb);

        List<Long> txnIDs = new ArrayList<>();
        txnIDs.add(1L);

        // apply log entry
        applier.applyBatch(ls.multiRead(txnIDs));

        for (int i = 0; i < 10; ++i) {
            assertEquals("v" + i, vs.get(vertex.get(i)));
            assertEquals("e" + i, es.get(edge.get(i)));
        }
    }

    @Test
    void testMore() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "log-applier-test-more", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        var vs = new VertexTemporalPropertyStore(graph0, baseDir + dataDir + "/vertex", false);
        var es = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir + "/edge", false);
        var ls = new LogStore(graph0, baseDir + dataDir + "/log");

        var applier = new LogApplier(vs, es);

        // copy from LogStoreTest
        List<VertexTemporalPropertyKey> vertex = new ArrayList<>();
        List<EdgeTemporalPropertyKey> edge = new ArrayList<>();

        // vertex 1, write three timestamps.
        vertex.add(makeVertex(1, 1));
        vertex.add(makeVertex(1, 2));
        vertex.add(makeVertex(1, 3));
        // vertex 2, write two timestamps.
        vertex.add(makeVertex(2, 4));
        vertex.add(makeVertex(2, 5));

        // edge 1->2, write three timestamps.
        edge.add(makeEdge(1, 2, 6));
        edge.add(makeEdge(1, 2, 7));
        edge.add(makeEdge(1, 2, 8));


        var wb1 = ls.startBatchWrite();

        int ind = 1;
        for (var v : vertex) {
            wb1.append(LogEntry.putVertex(v, "v" + ind));
            ++ind;
        }
        for (var e : edge) {
            wb1.append(LogEntry.putEdge(e, "e" + ind));
            ++ind;
        }

        long txnID = 1;
        ls.commitBatchWrite(txnID, wb1);
        List<Long> txnIDs = new ArrayList<>();
        txnIDs.add(txnID);

        applier.applyBatch(ls.multiRead(txnIDs));

        assertEquals("v1", vs.get(vertex.get(0)));
        assertEquals("v2", vs.get(vertex.get(1)));
        assertEquals("v3", vs.get(vertex.get(2)));
        assertEquals("v4", vs.get(vertex.get(3)));
        assertEquals("v5", vs.get(vertex.get(4)));

        assertEquals("e6", es.get(edge.get(0)));
        assertEquals("e7", es.get(edge.get(1)));
        assertEquals("e8", es.get(edge.get(2)));


        var wb2 = ls.startBatchWrite();
        // delete single, leave 7
        wb2.append(LogEntry.removeEdge(makeEdge(1, 2, 6)));
        wb2.append(LogEntry.removeEdge(makeEdge(1, 2, 8)));
        // delete all vertex 1, leave 2
        wb2.append(LogEntry.removeVertexPrefix(makeVertexPrefix(1)));

        txnID = 2;
        ls.commitBatchWrite(txnID, wb2);
        txnIDs.clear();
        txnIDs.add(txnID);

        applier.applyBatch(ls.multiRead(txnIDs));

        assertNull(es.get(makeEdge(1, 2, 6)));
        assertEquals("e7", es.get(makeEdge(1, 2, 7)));
        assertEquals("e7", es.get(makeEdge(1, 2, 8)));

        assertNull(vs.get(makeVertex(1, 1)));
        assertNull(vs.get(makeVertex(1, 2)));
        assertNull(vs.get(makeVertex(1, 3)));


    }

    @Test
    void testDeleteRange() {
        GraphSpaceID graph0 = new GraphSpaceID(1, "log-applier-test-more", "");
        String baseDir = "/Users/crusher/test/";
        String dataDir = graph0.getGraphName();
        var vs = new VertexTemporalPropertyStore(graph0, baseDir + dataDir + "/vertex", false);
        var es = new EdgeTemporalPropertyStore(graph0, baseDir + dataDir + "/edge", false);
        var ls = new LogStore(graph0, baseDir + dataDir + "/log");

        var applier = new LogApplier(vs, es);

        List<VertexTemporalPropertyKey> vertex = new ArrayList<>();
        for (int i = 0; i < 20; ++i) {
            vertex.add(makeVertex(1, i));
        }

        var wb1 = ls.startBatchWrite();

        for (var v : vertex) {
            wb1.append(LogEntry.putVertex(v, "v" + v.getTimestamp()));
        }

        long txnID = 1;
        ls.commitBatchWrite(txnID, wb1);
        List<Long> txnIDs = new ArrayList<>();
        txnIDs.add(txnID);

        applier.applyBatch(ls.multiRead(txnIDs));

        for (var v : vertex) {
            assertEquals("v" + v.getTimestamp(), vs.get(v));
        }

        var wb2 = ls.startBatchWrite();

        wb2.append(LogEntry.removeVertexRange(makeVertex(1, 5), makeVertex(1, 15)));
        txnID = 2;
        ls.commitBatchWrite(txnID, wb2);

        txnIDs.clear();
        txnIDs.add(txnID);

        applier.applyBatch(ls.multiRead(txnIDs));

        // after remove [5, 15), we get "v4"
        for (int i = 6; i < 15; ++i) {
            assertEquals("v4", vs.get(makeVertex(1, i)));
        }

    }


}
