package cn.edu.buaa.act.tgraph.kvstore;

import cn.edu.buaa.act.tgraph.impl.tgraphdb.GraphSpaceID;

public class StoreOptions {
    private GraphSpaceID graph = null;
    private String dataPath = null;
    private boolean readonly = false;
    private Comparator comparator = null;

    private StoreOptions(GraphSpaceID graph, String dataPath, boolean readonly, Comparator comparator) {
        this.graph = graph;
        this.dataPath = dataPath;
        this.readonly = readonly;
        this.comparator = comparator;
    }

    public static StoreOptions of(GraphSpaceID graph, String dataPath, boolean readonly, Comparator comparator) {
        return new StoreOptions(graph, dataPath, readonly, comparator);
    }

    public static StoreOptions of(GraphSpaceID graph, String dataPath, boolean readonly) {
        return new StoreOptions(graph, dataPath, readonly, null);
    }

    public static StoreOptions of(GraphSpaceID graph, String dataPath) {
        return new StoreOptions(graph, dataPath, false, null);
    }


    public GraphSpaceID getGraph() {
        return graph;
    }

    public void setGraph(GraphSpaceID graph) {
        this.graph = graph;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public boolean isReadonly() {
        return readonly;
    }

    public void setReadonly(boolean readonly) {
        this.readonly = readonly;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }
}
