package txn;

import api.tgraphdb.Node;
import api.tgraphdb.Relationship;
import impl.tgraphdb.Edge;
import impl.tgraphdb.TGraphConfig;
import api.tgraphdb.Transaction;
import impl.tgraphdb.Vertex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.*;
import property.EdgeTemporalPropertyStore;
import property.EdgeTemporalPropertyWriteBatch;
import property.VertexTemporalPropertyStore;
import property.VertexTemporalPropertyWriteBatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

// In fact, in Neo4j function api, entities are act as executor,
// in this way, transaction should pass lock/log manager to entities.
// In Neo4j, Transaction Class encapsulates too many things, but we
// have no idea for the compatibility.
// txn must be hold in one and only one thread.

public class TransactionImpl implements Transaction {

    private static final Log log = LogFactory.getLog(TransactionImpl.class);

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

    // Transaction object keeps track of all its temporal property locks.
    private final HashSet<TemporalPropertyID> sharedLockSet = new HashSet<>();
    private final HashSet<TemporalPropertyID> exclusiveLockSet = new HashSet<>();

    // entity executor context
    private final EntityExecutorContext exeCtx;

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

        exeCtx = new EntityExecutorContext(txnID, this.txnManager.getLockManager(), this.logWb);
    }

    // NOTE!: this api is exposed only for LockManager ut.
    // NOTE!: Don't use it.
    // TODO(crusher): It's ugly here, we can abstract some interface to avoid it.
    public TransactionImpl(long txnID) {
        log.info("NOTE!: you create a transaction using a test only api.");
        this.txnID = txnID;
        this.state = TransactionState.ACTIVE;

        this.graphTxn = null;
        this.vertex = null;
        this.edge = null;
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

    boolean holdSLock(TemporalPropertyID tp) {
        return sharedLockSet.contains(tp);
    }

    boolean holdXLock(TemporalPropertyID tp) {
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

    //private ResourceIterator<Node> resourceIteratorWrapper(ResourceIterator<org.neo4j.graphdb.Relationship> neoNodes) {
    //    return new ResourceIterator<>() {
    //        @Override
    //        public void close() {
    //            neoNodes.close();
    //        }

    //        @Override
    //        public boolean hasNext() {
    //            return neoNodes.hasNext();
    //        }

    //        @Override
    //        public Node next() {
    //            var neoNode = neoNodes.next();
    //            return new Vertex(neoNode, exeCtx);
    //        }
    //    };
    //}

    //private ResourceIterator<Edge> resourceIteratorWrapper(ResourceIterator<org.neo4j.graphdb.Relationship> neoRels) {
    //    return new ResourceIterator<>() {
    //        @Override
    //        public void close() {
    //            neoRels.close();
    //        }

    //        @Override
    //        public boolean hasNext() {
    //            return neoRels.hasNext();
    //        }

    //        @Override
    //        public Edge next() {
    //            var neoRel = neoRels.next();
    //            return new Edge(neoRel, exeCtx);
    //        }
    //    };
    //}


    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, String template, StringSearchMode searchMode) {
        var neoNodes = graphTxn.findNodes(label, key, template, searchMode);
        //return resourceIteratorWrapper(neoNodes);
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues) {
        var neoNodes = graphTxn.findNodes(label, propertyValues);
        // return resourceIteratorWrapper(neoNodes);
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        // var neoNodes = graphTxn.findNodes(label, key1, value1, key2, value2, key3, value3);
        // return resourceIteratorWrapper(neoNodes);
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2) {
        //var neoNodes = graphTxn.findNodes(label, key1, value1, key2, value2);
        //return resourceIteratorWrapper(neoNodes);
        return null;
    }

    @Override
    public Node findNode(Label label, String key, Object value) {
        var neoNode = graphTxn.findNode(label, key, value);
        return new Vertex(neoNode, exeCtx);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, Object value) {
        var neoNodes = graphTxn.findNodes(label, key, value);
        //return resourceIteratorWrapper(neoNodes);
        return null;
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label) {
        var neoNodes = graphTxn.findNodes(label);
        // return resourceIteratorWrapper(neoNodes);
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

}
