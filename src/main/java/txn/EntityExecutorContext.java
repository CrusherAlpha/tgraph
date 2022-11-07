package txn;

import org.neo4j.graphdb.Transaction;
import property.EdgeTemporalPropertyStore;
import property.EdgeTemporalPropertyWriteBatch;
import property.VertexTemporalPropertyStore;
import property.VertexTemporalPropertyWriteBatch;

// Executor should surround by txn.
public class EntityExecutorContext {

    // info
    // belong to which txnID
    private final long txnID;

    // surround ops in transaction
    private final TransactionManager txnManager;

    // for schema change through multi-level lock(IS, IX)
    private final org.neo4j.graphdb.Transaction graphTxn;

    // for log write
    private final LogWriteBatch logWb;

    // for temporal property write
    private final VertexTemporalPropertyWriteBatch vertexWb;
    private final EdgeTemporalPropertyWriteBatch edgeWb;


    // for temporal property read
    private final VertexTemporalPropertyStore vertex;
    private final EdgeTemporalPropertyStore edge;


    public EntityExecutorContext(long txnID, TransactionManager txnManager, Transaction graphTxn,
                                 LogWriteBatch logWb, VertexTemporalPropertyWriteBatch vertexWb, EdgeTemporalPropertyWriteBatch edgeWb,
                                 VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge) {
        this.txnID = txnID;
        this.txnManager = txnManager;
        this.graphTxn = graphTxn;
        this.logWb = logWb;
        this.vertexWb = vertexWb;
        this.edgeWb = edgeWb;
        this.vertex = vertex;
        this.edge = edge;
    }

    public void releaseLocks() {
        txnManager.releaseLocks(txnID);
    }

    public long getTxnID() {
        return txnID;
    }

    public TransactionManager getTxnManager() {
        return txnManager;
    }

    public Transaction getGraphTxn() {
        return graphTxn;
    }

    public LogWriteBatch getLogWb() {
        return logWb;
    }

    public VertexTemporalPropertyWriteBatch getVertexWb() {
        return vertexWb;
    }

    public EdgeTemporalPropertyWriteBatch getEdgeWb() {
        return edgeWb;
    }

    public VertexTemporalPropertyStore getVertex() {
        return vertex;
    }

    public EdgeTemporalPropertyStore getEdge() {
        return edge;
    }
}
