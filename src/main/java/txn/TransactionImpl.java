package txn;

import api.tgraphdb.Node;
import api.tgraphdb.Relationship;
import impl.tgraphdb.TGraphConfig;
import api.tgraphdb.Transaction;
import org.neo4j.graphdb.*;
import property.EdgeTemporalPropertyStore;
import property.EdgeTemporalPropertyWriteBatch;
import property.VertexTemporalPropertyStore;
import property.VertexTemporalPropertyWriteBatch;

import java.util.HashSet;
import java.util.Map;

// In fact, in Neo4j function api, entities are act as executor,
// in this way, transaction should pass lock/log manager to entities.
// In Neo4j, Transaction Class encapsulates too many things, but we
// have no idea for the compatibility.
// txn must be hold in one and only one thread.

public class TransactionImpl implements Transaction {

    // info
    private final long txnID;
    private TransactionState state;
    // TODO(crusher): maybe we should record this transaction belongs to which thread.

    // store
    private final org.neo4j.graphdb.Transaction graphTxn;
    private final VertexTemporalPropertyStore vertex;
    private final EdgeTemporalPropertyStore edge;

    // hold txn manager reference to commit or abort txn.
    private final TransactionManager txnManager;

    // transaction private space
    // write batch acts as transaction private space.
    private final VertexTemporalPropertyWriteBatch vertexWb;
    private final EdgeTemporalPropertyWriteBatch edgeWb;
    private final LogWriteBatch logWb;

    // Transaction object keeps track of all its time point temporal property locks.
    private final HashSet<TimePointTemporalPropertyID> sharedLockSet = new HashSet<>();
    private final HashSet<TimePointTemporalPropertyID> exclusiveLockSet = new HashSet<>();

    public TransactionImpl(long txnID, org.neo4j.graphdb.Transaction graphTxn, VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge, TransactionManager txnManager) {
        this.txnID = txnID;
        this.state = TransactionState.ACTIVE;

        this.graphTxn = graphTxn;
        this.vertex = vertex;
        this.edge = edge;

        this.txnManager = txnManager;

        this.vertexWb = vertex.startBatchWrite();
        this.edgeWb = edge.startBatchWrite();
        this.logWb = this.txnManager.getLogStore().startBatchWrite();
    }

    public LogWriteBatch getLogWb() {
        return logWb;
    }

    public HashSet<TimePointTemporalPropertyID> getSharedLockSet() {
        return sharedLockSet;
    }

    public HashSet<TimePointTemporalPropertyID> getExclusiveLockSet() {
        return exclusiveLockSet;
    }

    public TransactionState getState() {
        return state;
    }

    public void setState(TransactionState state) {
        this.state = state;
    }

    boolean holdSLock(TimePointTemporalPropertyID tp) {
        return sharedLockSet.contains(tp);
    }

    boolean holdXLock(TimePointTemporalPropertyID tp) {
        return exclusiveLockSet.contains(tp);
    }


    public long getTxnID() {
        return txnID;
    }

    public void writeCommitLog() {
        graphTxn.createNode(Label.label(TGraphConfig.COMMIT_LOG_NODE_LABEL));
    }

    public VertexTemporalPropertyWriteBatch getVertexWb() {
        return vertexWb;
    }

    public EdgeTemporalPropertyWriteBatch getEdgeWb() {
        return edgeWb;
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
        txnManager.commitTransaction(this);
    }

    @Override
    public void rollback() {
        txnManager.abortTransaction(this);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean prepare() {
        return false;
    }
}
