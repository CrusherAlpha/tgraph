package impl.tgraphdb;

import api.tgraphdb.TGraphDatabaseService;
import api.tgraphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;
import property.EdgeTemporalPropertyStore;
import property.VertexTemporalPropertyStore;
import txn.TransactionManager;

import java.util.concurrent.TimeUnit;

// TGraphDatabase:
//      Store: hold GraphStore(Neo4j), VertexTemporalPropertyStore, EdgeTemporalPropertyStore
//      Transaction: TransactionManager
// TODO(crusher): purge thread and background thread.
public class TGraphDatabase implements TGraphDatabaseService {

    // graph identifier.
    private final GraphSpaceID graphSpaceID;

    // store
    private final GraphDatabaseService graph;
    private final VertexTemporalPropertyStore vertex;
    private final EdgeTemporalPropertyStore edge;

    // transaction
    private final TransactionManager txnManager;

    // GraphDatabaseService is acquired through neo4j dbms, thus should be passed into Constructor.
    // GraphSpaceID is managed by dbms, thus should be passed into Constructor.
    public TGraphDatabase(GraphSpaceID graphSpaceID, GraphDatabaseService graph) {
        this.graphSpaceID = graphSpaceID;
        this.graph = graph;
        this.vertex = new VertexTemporalPropertyStore(graphSpaceID, graphSpaceID.getDatabasePath() + "/vertex", false);
        this.edge = new EdgeTemporalPropertyStore(graphSpaceID, graphSpaceID.getDatabasePath() + "/edge", false);
        this.txnManager = new TransactionManager(graphSpaceID);
    }

    @Override
    public boolean isAvailable(long timeout) {
        return graph.isAvailable(timeout);
    }

    @Override
    public Transaction beginTx() {
        return null;
    }

    @Override
    public Transaction beginTx(long timeout, TimeUnit unit) {
        return null;
    }

    @Override
    public String databaseName() {
        return graph.databaseName();
    }
}
