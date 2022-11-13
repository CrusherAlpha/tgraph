package cn.edu.buaa.act.tgraph.impl.tgraphdb;

import cn.edu.buaa.act.tgraph.api.tgraphdb.TGraphDatabaseService;
import cn.edu.buaa.act.tgraph.api.tgraphdb.Transaction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import cn.edu.buaa.act.tgraph.property.EdgeTemporalPropertyStore;
import cn.edu.buaa.act.tgraph.property.VertexTemporalPropertyStore;
import cn.edu.buaa.act.tgraph.txn.TransactionManager;


// TGraphDatabase:
//      Store: hold GraphStore(Neo4j), VertexTemporalPropertyStore, EdgeTemporalPropertyStore
//      Transaction: TransactionManager

// Note!: You should get TGraphDatabase through DatabaseManager not call Constructor directly.

public class TGraphDatabase implements TGraphDatabaseService {

    private static final Log log = LogFactory.getLog(TGraphDatabase.class);

    // info
    private final GraphSpaceID id;
    // graph store
    private final GraphDatabaseService graph;

    // entity temporal store
    private final VertexTemporalPropertyStore vertex;
    private final EdgeTemporalPropertyStore edge;

    // transaction
    private final TransactionManager txnManager;

    // GraphDatabaseService is acquired through neo4j dbms, thus should be passed into Constructor.
    // GraphSpaceID is managed by dbms, thus should be passed into Constructor.
    public TGraphDatabase(GraphSpaceID graphSpaceID, GraphDatabaseService graph) {
        this.id = graphSpaceID;

        // graph identifier.
        this.graph = graph;
        this.vertex = new VertexTemporalPropertyStore(graphSpaceID, graphSpaceID.getDatabasePath() + "/vertex-tp-data", false);
        this.edge = new EdgeTemporalPropertyStore(graphSpaceID, graphSpaceID.getDatabasePath() + "/edge-tp-data", false);
        this.txnManager = new TransactionManager(graphSpaceID, graph, this.vertex, this.edge);
        // start recovery
        this.txnManager.recover();
        // after recovery, start txn manager background task and purge task
        this.txnManager.start();
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
        return id.getGraphName();
    }

    @Override
    public void shutdown() {
        try {
            txnManager.close();
            vertex.stop();
            edge.stop();
        } catch (InterruptedException e) {
            log.error("tg close failed.");
            e.printStackTrace();
        }
    }

    @Override
    public void drop() {
        txnManager.drop();
        vertex.drop();
        edge.drop();
    }
}
