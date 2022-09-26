package impl.tgraphdb;

import api.tgraphdb.TGraphDatabaseService;
import api.tgraphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;
import property.EdgeTemporalPropertyStore;
import property.VertexTemporalPropertyStore;
import txn.LogStore;

import java.util.concurrent.TimeUnit;

// TGraphDatabase hold GraphDatabaseService(Neo4j), VertexTemporalPropertyStore, EdgeTemporalPropertyStore, LogStore
public class TGraphDatabase implements TGraphDatabaseService {

    private final GraphDatabaseService graph;
    private final VertexTemporalPropertyStore vertex;
    private final EdgeTemporalPropertyStore edge;
    private final LogStore logStore;
    private final String name;

    public TGraphDatabase(String databaseName, GraphDatabaseService graph, VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge, LogStore logStore) {
        this.graph = graph;
        this.vertex = vertex;
        this.edge = edge;
        this.logStore = logStore;
        this.name = databaseName;
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
        return name;
    }
}
