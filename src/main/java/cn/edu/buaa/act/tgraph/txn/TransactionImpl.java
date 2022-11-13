package cn.edu.buaa.act.tgraph.txn;

import cn.edu.buaa.act.tgraph.api.tgraphdb.Node;
import cn.edu.buaa.act.tgraph.api.tgraphdb.Relationship;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.Edge;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.TGraphConfig;
import cn.edu.buaa.act.tgraph.api.tgraphdb.Transaction;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.Vertex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.*;
import cn.edu.buaa.act.tgraph.property.EdgeTemporalPropertyStore;
import cn.edu.buaa.act.tgraph.property.EdgeTemporalPropertyWriteBatch;
import cn.edu.buaa.act.tgraph.property.VertexTemporalPropertyStore;
import cn.edu.buaa.act.tgraph.property.VertexTemporalPropertyWriteBatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


// In fact, in Neo4j function api, entities are act as executor,
// in this way, transaction should pass lock/log manager to entities.
// In Neo4j, Transaction Class encapsulates too many things, but we
// have no idea for the compatibility.

// NOTE!: Txn must be hold in one and only one thread.

public class TransactionImpl implements Transaction {

    private static final Log log = LogFactory.getLog(TransactionImpl.class);

    // info
    private final long txnID;
    private TransactionState state;
    // TODO(crusher): maybe we should record this transaction belongs to which thread.

    // store
    private final org.neo4j.graphdb.Transaction graphTxn;

    // hold txn manager reference to commit or abort txn.
    private final TransactionManager txnManager;

    // transaction private space
    // write batch acts as transaction private space.
    private final VertexTemporalPropertyWriteBatch vertexWb;
    private final EdgeTemporalPropertyWriteBatch edgeWb;
    private final LogWriteBatch logWb;

    // Transaction object keeps track of all its temporal property locks.
    private final HashSet<TemporalPropertyID> sharedLockSet = new HashSet<>();
    private final HashSet<TemporalPropertyID> exclusiveLockSet = new HashSet<>();

    // entity executor context
    private final EntityExecutorContext exeCtx;

    public TransactionImpl(long txnID, org.neo4j.graphdb.Transaction graphTxn, VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge, TransactionManager txnManager) {
        this.txnID = txnID;
        this.state = TransactionState.ACTIVE;

        this.graphTxn = graphTxn;

        this.txnManager = txnManager;

        this.vertexWb = vertex.startBatchWrite();
        this.edgeWb = edge.startBatchWrite();
        this.logWb = this.txnManager.getLogStore().startBatchWrite();

        exeCtx = new EntityExecutorContext(txnID, txnManager, graphTxn, this.logWb, this.vertexWb,this.edgeWb, vertex, edge);
    }

    // NOTE!: this api is exposed only for LockManager ut.
    // NOTE!: Don't use it.
    // TODO(crusher): It's ugly here, we can abstract some interface to avoid it.
    public TransactionImpl(long txnID) {
        log.info("NOTE!: you create a transaction using a test only api.");
        this.txnID = txnID;
        this.state = TransactionState.ACTIVE;

        this.graphTxn = null;
        this.txnManager = null;
        this.vertexWb = null;
        this.edgeWb = null;
        this.logWb = null;
        exeCtx = null;
    }

    public LogWriteBatch getLogWb() {
        return logWb;
    }

    public HashSet<TemporalPropertyID> getSharedLockSet() {
        return sharedLockSet;
    }

    public HashSet<TemporalPropertyID> getExclusiveLockSet() {
        return exclusiveLockSet;
    }

    public TransactionState getState() {
        return state;
    }

    public void setState(TransactionState state) {
        this.state = state;
    }

    public boolean holdSLock(TemporalPropertyID tp) {
        return sharedLockSet.contains(tp);
    }

    public boolean holdXLock(TemporalPropertyID tp) {
        return exclusiveLockSet.contains(tp);
    }


    public long getTxnID() {
        return txnID;
    }

    public void writeCommitLog() {
        var node = graphTxn.createNode(Label.label(TGraphConfig.COMMIT_LOG_NODE_LABEL));
        node.setProperty(TGraphConfig.COMMIT_LOG_TXN_IDENTIFIER, txnID);
        graphTxn.commit();
    }

    public VertexTemporalPropertyWriteBatch getVertexWb() {
        return vertexWb;
    }

    public EdgeTemporalPropertyWriteBatch getEdgeWb() {
        return edgeWb;
    }

    @Override
    public Node createNode() {
        org.neo4j.graphdb.Node neoNode = graphTxn.createNode();
        return new Vertex(neoNode, exeCtx);
    }

    @Override
    public Node createNode(Label... labels) {
        org.neo4j.graphdb.Node neoNode = graphTxn.createNode(labels);
        return new Vertex(neoNode, exeCtx);
    }

    @Override
    public Node getNodeById(long id) {
        org.neo4j.graphdb.Node neoNode = graphTxn.getNodeById(id);
        return new Vertex(neoNode, exeCtx);
    }

    @Override
    public Relationship getRelationshipById(long id) {
        org.neo4j.graphdb.Relationship neoRel = graphTxn.getRelationshipById(id);
        return new Edge(neoRel, exeCtx);
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return graphTxn.getAllLabelsInUse();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return graphTxn.getAllRelationshipTypesInUse();
    }

    @Override
    public Iterable<Label> getAllLabels() {
        return graphTxn.getAllLabels();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypes() {
        return graphTxn.getAllRelationshipTypes();
    }

    @Override
    public Iterable<String> getAllPropertyKeys() {
        var p = graphTxn.getAllPropertyKeys();
        List<String> ret = new ArrayList<>();
        for (var k : p) {
            if (k.startsWith(TGraphConfig.TEMPORAL_PROPERTY_PREFIX)) {
                ret.add(k.substring(TGraphConfig.TEMPORAL_PROPERTY_PREFIX.length()));
            } else {
                ret.add(k);
            }
        }
        return ret;
    }


    private ResourceIterator<Node> nodeResourceIteratorWrapper(ResourceIterator<org.neo4j.graphdb.Node> neoNodes) {
        return new ResourceIterator<>() {
            @Override
            public void close() {
                neoNodes.close();
            }

            @Override
            public boolean hasNext() {
                return neoNodes.hasNext();
            }

            @Override
            public Node next() {
                var neoNode = neoNodes.next();
                return new Vertex(neoNode, exeCtx);
            }
        };
    }

    private ResourceIterator<Relationship> relationshipResourceIteratorWrapper(ResourceIterator<org.neo4j.graphdb.Relationship> neoRels) {
        return new ResourceIterator<>() {
            @Override
            public void close() {
                neoRels.close();
            }

            @Override
            public boolean hasNext() {
                return neoRels.hasNext();
            }

            @Override
            public Edge next() {
                var neoRel = neoRels.next();
                return new Edge(neoRel, exeCtx);
            }
        };
    }


    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, String template, StringSearchMode searchMode) {
        var neoNodes = graphTxn.findNodes(label, key, template, searchMode);
        return nodeResourceIteratorWrapper(neoNodes);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues) {
        var neoNodes = graphTxn.findNodes(label, propertyValues);
        return nodeResourceIteratorWrapper(neoNodes);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        var neoNodes = graphTxn.findNodes(label, key1, value1, key2, value2, key3, value3);
        return nodeResourceIteratorWrapper(neoNodes);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2) {
        var neoNodes = graphTxn.findNodes(label, key1, value1, key2, value2);
        return nodeResourceIteratorWrapper(neoNodes);
    }

    @Override
    public Node findNode(Label label, String key, Object value) {
        var neoNode = graphTxn.findNode(label, key, value);
        return new Vertex(neoNode, exeCtx);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, Object value) {
        var neoNodes = graphTxn.findNodes(label, key, value);
        return nodeResourceIteratorWrapper(neoNodes);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label) {
        var neoNodes = graphTxn.findNodes(label);
        return nodeResourceIteratorWrapper(neoNodes);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key, String template, StringSearchMode searchMode) {
        var neoRels = graphTxn.findRelationships(relationshipType, key, template, searchMode);
        return relationshipResourceIteratorWrapper(neoRels);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, Map<String, Object> propertyValues) {
        var neoRels = graphTxn.findRelationships(relationshipType, propertyValues);
        return relationshipResourceIteratorWrapper(neoRels);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        var neoRels = graphTxn.findRelationships(relationshipType, key1, value1, key2, value2, key3, value3);
        return relationshipResourceIteratorWrapper(neoRels);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key1, Object value1, String key2, Object value2) {
        var neoRels = graphTxn.findRelationships(relationshipType, key1, value1, key2, value2);
        return relationshipResourceIteratorWrapper(neoRels);
    }

    @Override
    public Relationship findRelationship(RelationshipType relationshipType, String key, Object value) {
        var neoRel = graphTxn.findRelationship(relationshipType, key, value);
        return new Edge(neoRel, exeCtx);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType, String key, Object value) {
        var neoRels = graphTxn.findRelationships(relationshipType, key, value);
        return relationshipResourceIteratorWrapper(neoRels);
    }

    @Override
    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType) {
        var neoRels = graphTxn.findRelationships(relationshipType);
        return relationshipResourceIteratorWrapper(neoRels);
    }

    @Override
    public void terminate() {
        close();
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        var neoNodes = graphTxn.getAllNodes();
        return () -> nodeResourceIteratorWrapper(neoNodes.iterator());
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        var neoRels = graphTxn.getAllRelationships();
        return () -> relationshipResourceIteratorWrapper(neoRels.iterator());
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
        // if commit or abort already, do nothing.
        // or, just rollback this transaction.
        if (state == TransactionState.ACTIVE) {
            rollback();
        }
    }

}
