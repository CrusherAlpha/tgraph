package impl.tgraphdb;

import api.tgraphdb.Node;
import api.tgraphdb.Relationship;
import api.tgraphdb.Transaction;
import org.neo4j.graphdb.*;
import property.EdgeTemporalPropertyStore;
import property.VertexTemporalPropertyStore;
import txn.LogStore;

import java.util.Map;

public class PessimisticTransaction implements Transaction {

    private final org.neo4j.graphdb.Transaction graphTxn;
    private final VertexTemporalPropertyStore vertex;
    private final EdgeTemporalPropertyStore edge;
    private final LogStore logStore;

    public PessimisticTransaction(org.neo4j.graphdb.Transaction graphTxn, VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge, LogStore logStore) {
        this.graphTxn = graphTxn;
        this.vertex = vertex;
        this.edge = edge;
        this.logStore = logStore;
    }

    @Override
    public Node createNode() {
        return null;
    }

    @Override
    public Node createNode(Label... labels) {
        return null;
    }

    @Override
    public Node getNodeById(long id) {
        return null;
    }

    @Override
    public Relationship getRelationshipById(long id) {
        return null;
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return null;
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return null;
    }

    @Override
    public Iterable<Label> getAllLabels() {
        return null;
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypes() {
        return null;
    }

    @Override
    public Iterable<String> getAllPropertyKeys() {
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, String template, StringSearchMode searchMode) {
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues) {
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2) {
        return null;
    }

    @Override
    public Node findNode(Label label, String key, Object value) {
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, Object value) {
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label) {
        return null;
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key, String template, StringSearchMode searchMode) {
        return null;
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, Map<String, Object> propertyValues) {
        return null;
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        return null;
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key1, Object value1, String key2, Object value2) {
        return null;
    }

    @Override
    public Relationship findRelationship(RelationshipType relationshipType, String key, Object value) {
        return null;
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key, Object value) {
        return null;
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType) {
        return null;
    }

    @Override
    public void terminate() {

    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        return null;
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        return null;
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean prepare() {
        return false;
    }
}
