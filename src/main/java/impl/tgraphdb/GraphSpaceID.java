package impl.tgraphdb;

public class GraphSpaceID {
    // unique
    // Note!: 0 is reserved for meta db.
    private final int graphId;
    // unique, given by users, graphName == databaseName.
    private final String graphName;
    // unique
    private final String databasePath;

    public GraphSpaceID(int graphId, String graphName, String databasePath) {
        this.graphId = graphId;
        this.graphName = graphName;
        this.databasePath = databasePath;
    }


    public int getGraphId() {
        return graphId;
    }

    public String getGraphName() {
        return graphName;
    }

    public String getDatabasePath() {
        return databasePath;
    }
}
