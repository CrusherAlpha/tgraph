package impl.tgraphdb;

import api.tgraphdb.Node;
import api.tgraphdb.Relationship;
import common.Pair;
import org.neo4j.graphdb.RelationshipType;
import txn.EntityExecutorContext;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

// In Neo4j, Vertex/Edge is like Executor in RDMS.
public class Edge implements Relationship {
    private final org.neo4j.graphdb.Relationship neoEdge;
    private final EntityExecutorContext exeCtx;

    public Edge(org.neo4j.graphdb.Relationship neoEdge, EntityExecutorContext exeCtx) {
        this.neoEdge = neoEdge;
        this.exeCtx = exeCtx;
    }

    @Override
    public long getId() {
        return neoEdge.getId();
    }

    @Override
    public boolean hasProperty(String key) {
        return false;
    }

    @Override
    public Object getProperty(String key) {
        return null;
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {

    }

    @Override
    public Object removeProperty(String key) {
        return null;
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return null;
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        return null;
    }

    @Override
    public Map<String, Object> getAllProperties() {
        return null;
    }

    @Override
    public void createTemporalProperty(String key) {

    }

    @Override
    public boolean hasTemporalProperty(String key) {
        return false;
    }

    @Override
    public Object getTemporalPropertyValue(String key, Timestamp timestamp) {
        return null;
    }

    @Override
    public List<Pair<Timestamp, Object>> getTemporalPropertyValue(String key, Timestamp start, Timestamp end) {
        return null;
    }

    @Override
    public void setTemporalPropertyValue(String key, Timestamp timestamp, Object value) {

    }

    @Override
    public void setTemporalPropertyValue(String key, Timestamp start, Timestamp end, Object value) {

    }

    @Override
    public void removeTemporalProperty(String key) {

    }

    @Override
    public Object removeTemporalPropertyValue(String key, Timestamp timestamp) {
        return null;
    }

    @Override
    public List<Pair<Timestamp, Object>> removeTemporalPropertyValue(String key, Timestamp start, Timestamp end) {
        return null;
    }

    @Override
    public List<Pair<Timestamp, Object>> removeTemporalPropertyValue(String key) {
        return null;
    }

    @Override
    public Iterable<String> getTemporalPropertyKeys() {
        return null;
    }

    @Override
    public void delete() {

    }

    @Override
    public Node getStartNode() {
        return null;
    }

    @Override
    public Node getEndNode() {
        return null;
    }

    @Override
    public Node getOtherNode(Node node) {
        return null;
    }

    @Override
    public Node[] getNodes() {
        return new Node[0];
    }

    @Override
    public RelationshipType getType() {
        return neoEdge.getType();
    }

    @Override
    public boolean isType(RelationshipType type) {
        return neoEdge.isType(type);
    }
}
