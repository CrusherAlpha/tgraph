package txn;

import impl.tgraphdb.GraphSpaceID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;
import property.EdgeTemporalPropertyKey;
import property.VertexTemporalPropertyKey;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LogStoreTest {
    private static final Log log = LogFactory.getLog(LogStoreTest.class);


    private VertexTemporalPropertyKey makeVertex(long nodeId, long timestamp) {
        return new VertexTemporalPropertyKey(nodeId, "test-vp", timestamp);
    }

    private EdgeTemporalPropertyKey makeEdge(long startId, long endId, long timestamp) {
        return new EdgeTemporalPropertyKey(startId, endId, "test-ep", timestamp);
    }

    @Test
    void testBase() {
        GraphSpaceID graph0 = new GraphSpaceID(0, "test-log-store-base", "");
        String baseDir = "/Users/crusher/test/";
        LogStore ls = new LogStore(graph0, baseDir);

        List<VertexTemporalPropertyKey> vertex = new ArrayList<>();
        List<EdgeTemporalPropertyKey> edge = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            vertex.add(makeVertex(i, i));
            edge.add(makeEdge(i, i+1, i));
        }

        var wb = ls.startBatchWrite();

        int ind = 0;
        for (var v : vertex) {
            wb.append(LogEntry.fromVertex(v, "v" + ind));
            ++ind;
        }

        ind = 0;
        for (var e : edge) {
            wb.append(LogEntry.fromEdge(e, "e" + ind));
            ++ind;
        }

        ls.commitBatchWrite(1, wb);

        List<Long> txnIDs = new ArrayList<>();
        txnIDs.add(1L);

        var en = ls.multiRead(txnIDs);
        assertEquals(1, en.size());
        var logs = en.get(0).getLogs();

        assertEquals(20, logs.size());

    }

    @Test
    void testMore() {
        String baseDir = "/Users/crusher/test/";
        GraphSpaceID graph0 = new GraphSpaceID(0, "test-log-store-more", baseDir);
        LogStore ls = new LogStore(graph0, baseDir);

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

        var wb = ls.startBatchWrite();

        int ind = 1;
        for (var v : vertex) {
            wb.append(LogEntry.fromVertex(v, "v" + ind));
            ++ind;
        }
        for (var e : edge) {
            wb.append(LogEntry.fromEdge(e, "e" + ind));
            ++ind;
        }

        long txnID = 1;
        ls.commitBatchWrite(txnID, wb);

        List<Long> txnIDs = new ArrayList<>();
        txnIDs.add(txnID);

        var en = ls.multiRead(txnIDs);
        assertEquals(1, en.size());
        var logs = en.get(0).getLogs();
        assertEquals(8, logs.size());

        ind = 1;
        // vertex 5, edge 3.
        for (var log : logs) {
            if (ind <= 5) {
                var ver = log.toVertex();
                var k = ver.first();
                var v = (String) ver.second();
                assertEquals(ind, k.getTimestamp());
                assertEquals("v" + ind, v);
            } else {
                var ede = log.toEdge();
                var k = ede.first();
                var v = ede.second();
                assertEquals(ind, k.getTimestamp());
                assertEquals("e" + ind, v);
            }
            ++ind;
        }

    }

    @Test
    void testDelete() {

        long txnID = 1;
        List<Long> txnIDs = new ArrayList<>();
        txnIDs.add(txnID);

        String baseDir = "/Users/crusher/test/";
        GraphSpaceID graph0 = new GraphSpaceID(0, "test-log-store-delete", baseDir);
        LogStore ls = new LogStore(graph0, baseDir);

        List<VertexTemporalPropertyKey> vertex = new ArrayList<>();
        List<EdgeTemporalPropertyKey> edge = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            vertex.add(makeVertex(i, i));
            edge.add(makeEdge(i, i+1, i));
        }

        var wb = ls.startBatchWrite();

        int ind = 0;
        for (var v : vertex) {
            wb.append(LogEntry.fromVertex(v, "v" + ind));
            ++ind;
        }

        for (var e : edge) {
            wb.append(LogEntry.fromEdge(e, "e" + ind));
            ++ind;
        }

        // before commit, expect 0.
        var en = ls.multiRead(txnIDs);
        assertEquals(0, en.size());

        ls.commitBatchWrite(1, wb);

        // after commit, expect 1.
        en = ls.multiRead(txnIDs);
        assertEquals(1, en.size());
        var logs = en.get(0).getLogs();
        assertEquals(20, logs.size());

        // after delete, expect 0.
        ls.multiDelete(txnIDs);
        en = ls.multiRead(txnIDs);
        assertEquals(0, en.size());

    }
}
