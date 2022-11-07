package txn;

import com.google.common.base.Preconditions;
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
        if (entries.isEmpty()) {
            return;
        }
        try (var vertexWb = vertex.startBatchWrite(); var edgeWb = edge.startBatchWrite()) {
            for (var entry : entries) {
                for (var log : entry.getLogs()) {
                    if (log.entityType() == LogEntryEntityType.VERTEX) {
                        switch (log.redoLogType()) {
                            case APPEND: {
                                var pr = log.toVertex();
                                vertexWb.put(pr.first(), pr.second());
                                break;
                            }
                            case DELETE_SINGLE: {
                                var pr = log.toVertex();
                                vertexWb.remove(pr.first());
                                break;
                            }
                            case DELETE_RANGE: {
                                var pr = log.toVertexRange();
                                vertexWb.removeRange(pr.first(), pr.second());
                                break;
                            }
                            case DELETE_ALL: {
                                var pre = log.toVertexPrefix();
                                vertexWb.removePrefix(pre);
                                break;
                            }
                        }
                    } else {
                        Preconditions.checkState(log.entityType() == LogEntryEntityType.EDGE);
                        switch (log.redoLogType()) {
                            case APPEND: {
                                var pr = log.toEdge();
                                edgeWb.put(pr.first(), pr.second());
                                break;
                            }
                            case DELETE_SINGLE: {
                                var pr = log.toEdge();
                                edgeWb.remove(pr.first());
                                break;
                            }
                            case DELETE_RANGE: {
                                var pr = log.toEdgeRange();
                                edgeWb.removeRange(pr.first(), pr.second());
                                break;
                            }
                            case DELETE_ALL: {
                                var pre = log.toEdgePrefix();
                                edgeWb.removePrefix(pre);
                                break;
                            }
                        }

                    }
                }
                vertex.commitBatchWrite(vertexWb, false, true, true);
                edge.commitBatchWrite(edgeWb, false, true, true);
            }
        }
    }

}
