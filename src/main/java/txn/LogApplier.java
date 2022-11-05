package txn;

import property.EdgeTemporalPropertyStore;
import property.VertexTemporalPropertyStore;

import java.util.List;

// for transaction manager executes failure recovery

public class LogApplier {
    final VertexTemporalPropertyStore vertex;
    final EdgeTemporalPropertyStore edge;

    public LogApplier(VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge) {
        this.vertex = vertex;
        this.edge = edge;
    }

    public void applyBatch(List<LogWriteBatch> entries) {
        try (var vertexWb = vertex.startBatchWrite(); var edgeWb = edge.startBatchWrite()) {
            for (var entry : entries) {
                for (var log : entry.getLogs()) {
                    if (log.type() == LogEntryType.VERTEX) {
                        var pr = log.toVertex();
                        vertexWb.put(pr.first(), pr.second());
                    } else {
                        var pr = log.toEdge();
                        edgeWb.put(pr.first(), pr.second());
                    }

                }
            }
            vertex.commitBatchWrite(vertexWb, false, true, true);
            edge.commitBatchWrite(edgeWb, false, true, true);
        }
    }
}
