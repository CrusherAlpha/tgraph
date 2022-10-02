package impl.tgraphdb;

import api.tgraphdb.Node;
import api.tgraphdb.Relationship;
import common.Pair;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import txn.EntityExecutorContext;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

// In Neo4j, Vertex/Edge is like Executor in RDMS.
public class Vertex implements Node {
    private final org.neo4j.graphdb.Node neoVertex;
    private final EntityExecutorContext exeCtx;

    public Vertex(org.neo4j.graphdb.Node neoVertex, EntityExecutorContext exeCtx) {
        this.neoVertex = neoVertex;
        this.exeCtx = exeCtx;
    }

    @Override
    public long getId() {
        return neoVertex.getId();
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
    public Iterable<Relationship> getRelationships() {
        return null;
    }

    @Override
    public boolean hasRelationship() {
        return false;
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType... types) {
        return null;
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
        return null;
    }

    @Override
    public boolean hasRelationship(RelationshipType... types) {
        return false;
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        return false;
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction dir) {
        return null;
    }

    @Override
    public boolean hasRelationship(Direction dir) {
        return false;
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        return null;
    }

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        return null;
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        return null;
    }

    @Override
    public int getDegree() {
        return 0;
    }

    @Override
    public int getDegree(RelationshipType type) {
        return 0;
    }

    @Override
    public int getDegree(Direction direction) {
        return 0;
    }

    @Override
    public int getDegree(RelationshipType type, Direction direction) {
        return 0;
    }

    @Override
    public void addLabel(Label label) {

    }

    @Override
    public void removeLabel(Label label) {

    }

    @Override
    public boolean hasLabel(Label label) {
        return false;
    }

    @Override
    public Iterable<Label> getLabels() {
        return null;
    }
}
