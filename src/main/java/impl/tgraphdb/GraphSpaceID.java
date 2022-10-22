package impl.tgraphdb;

public class GraphSpaceID {
    // not unique and used for denoting meta-db now, left for future use.
    private final long graphId;
    // unique, given by users, graphName == databaseName.
    private final String graphName;
    // unique
    private final String databasePath;

    public GraphSpaceID(long graphId, String graphName, String databasePath) {
        this.graphId = graphId;
        this.graphName = graphName;
        this.databasePath = databasePath;
    }


    public long getGraphId() {
        return graphId;
    }

    public String getGraphName() {
        return graphName;
    }

    public String getDatabasePath() {
        return databasePath;
    }
}
