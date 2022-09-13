package impl.tgraphdb;

public class GraphSpaceID {
    // unique
    private final int graphId;
    // not unique
    private final String graphName;

    GraphSpaceID(int graphId, String graphName) {
        this.graphId = graphId;
        this.graphName = graphName;
    }

    public int getGraphId() {
        return graphId;
    }

    public String getGraphName() {
        return graphName;
    }
}
