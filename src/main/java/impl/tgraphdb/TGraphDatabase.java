package impl.tgraphdb;

import api.tgraphdb.TGraphDatabaseService;
import api.tgraphdb.Transaction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import property.EdgeTemporalPropertyStore;
import property.VertexTemporalPropertyStore;
import txn.TransactionManager;


// TGraphDatabase:
//      Store: hold GraphStore(Neo4j), VertexTemporalPropertyStore, EdgeTemporalPropertyStore
//      Transaction: TransactionManager

// Note!: You should get TGraphDatabase through DatabaseManager not call Constructor directly.

public class TGraphDatabase implements TGraphDatabaseService {

    private static final Log log = LogFactory.getLog(TGraphDatabase.class);

    // graph store
    private final GraphDatabaseService graph;

    // transaction
    private final TransactionManager txnManager;

    // GraphDatabaseService is acquired through neo4j dbms, thus should be passed into Constructor.
    // GraphSpaceID is managed by dbms, thus should be passed into Constructor.
    public TGraphDatabase(GraphSpaceID graphSpaceID, GraphDatabaseService graph) {
        // graph identifier.
        this.graph = graph;
        VertexTemporalPropertyStore vertex = new VertexTemporalPropertyStore(graphSpaceID, graphSpaceID.getDatabasePath() + "/vertex", false);
        EdgeTemporalPropertyStore edge = new EdgeTemporalPropertyStore(graphSpaceID, graphSpaceID.getDatabasePath() + "/edge", false);
        this.txnManager = new TransactionManager(graphSpaceID, graph, vertex, edge);
        // start recovery
        this.txnManager.recover();
    }

    @Override
    public boolean isAvailable(long timeout) {
        return graph.isAvailable(timeout);
    }

    @Override
    public Transaction beginTx() {
        return txnManager.beginTransaction();
    }

    @Override
    public String databaseName() {
        return graph.databaseName();
    }

    @Override
    public void shutdown() {
        try {
            txnManager.close();
        } catch (Exception e) {
            log.info("Transaction manager close failed.");
            e.printStackTrace();
        }
    }
}
