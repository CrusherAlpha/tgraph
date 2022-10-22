package txn;

import property.EdgeTemporalPropertyStore;
import property.VertexTemporalPropertyStore;

import java.util.List;

// for transaction manager failure recovery

public class LogApplier {
    final VertexTemporalPropertyStore vertex;
    final EdgeTemporalPropertyStore edge;

    public LogApplier(VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge) {
        this.vertex = vertex;
        this.edge = edge;
    }

    public void applyBatch(List<LogEntry> entries) {
        try (var vertexWb = vertex.startBatchWrite(); var edgeWb = edge.startBatchWrite()) {
            for (var entry : entries) {
                if (entry.type() == LogEntryType.VERTEX) {
                    var pr = entry.toVertex();
                    vertexWb.put(pr.first(), pr.second());
                } else {
                    var pr = entry.toEdge();
                    edgeWb.put(pr.first(), pr.second());
                }
            }
            vertex.commitBatchWrite(vertexWb, false, true, true);
            edge.commitBatchWrite(edgeWb, false, true, true);
        }
    }
}
