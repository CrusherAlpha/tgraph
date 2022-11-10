package impl.tgraphdb;

import api.tgraphdb.Node;
import api.tgraphdb.Relationship;
import api.tgraphdb.TemporalPropertyExistsException;
import api.tgraphdb.TemporalPropertyNotExistsException;
import com.google.common.base.Preconditions;
import common.EntityUtil;
import common.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.RelationshipType;
import property.EdgeTemporalPropertyKey;
import property.EdgeTemporalPropertyKeyPrefix;
import txn.EntityExecutorContext;
import txn.LogEntry;
import txn.TemporalPropertyID;
import txn.TransactionAbortException;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// In Neo4j, Vertex/Edge is like Executor in RDMS.
// The difficulty of implementing Vertex/Edge is schema change.
// In fact, static property changed handled by Neo4j itself,
// TGraph is in charge of temporal property change, and we
// implement it through multi-level lock.
public class Edge implements Relationship {

    private static final Log log = LogFactory.getLog(Edge.class);


    public final org.neo4j.graphdb.Relationship neoEdge;
    private final EntityExecutorContext exeCtx;

    // info
    private final long id;
    private final long startId;
    private final long endId;

    public Edge(org.neo4j.graphdb.Relationship neoEdge, EntityExecutorContext exeCtx) {
        this.neoEdge = neoEdge;
        this.exeCtx = exeCtx;
        id = neoEdge.getId();
        startId = neoEdge.getStartNodeId();
        endId = neoEdge.getEndNodeId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return id == edge.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    private Lock acquireIS() {
        return exeCtx.getGraphTxn().acquireReadLock(neoEdge);
    }

    private Lock acquireIX() {
        return exeCtx.getGraphTxn().acquireWriteLock(neoEdge);
    }

    private void doAcquireSX(String tp, boolean share) throws TransactionAbortException {
        var lm = exeCtx.getTxnManager().getLockManager();
        var txn = exeCtx.getTxnManager().getTransaction(exeCtx.getTxnID());
        Preconditions.checkNotNull(txn, "All operations must be surrounded by transaction.");
        try {
            if (share) {
                lm.acquireShared(txn, TemporalPropertyID.edge(startId, endId, tp));
            } else {
                var t = TemporalPropertyID.edge(startId, endId, tp);
                // caller is in charge of lock upgrading.
                if (txn.holdSLock(t)) {
                    lm.upgrade(txn, t);
                } else {
                    lm.acquireExclusive(txn, t);
                }
            }
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            exeCtx.getTxnManager().releaseLocks(exeCtx.getTxnID());
            throw e;
        }
    }

    private void acquireS(String tp) throws TransactionAbortException {
        doAcquireSX(tp, true);
    }

    private void acquireX(String tp) throws TransactionAbortException {
        doAcquireSX(tp, false);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean hasProperty(String key) {
        return neoEdge.hasProperty(key);
    }

    @Override
    public Object getProperty(String key) {
        return neoEdge.getProperty(key);
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        return neoEdge.getProperty(key, defaultValue);
    }

    @Override
    public void setProperty(String key, Object value) {
        neoEdge.setProperty(key, value);
    }

    @Override
    public Object removeProperty(String key) {
        return neoEdge.removeProperty(key);
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return EntityUtil.staticPropertyKeysFilter(neoEdge.getPropertyKeys());
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        return neoEdge.getProperties(keys);
    }

    @Override
    public Map<String, Object> getAllProperties() {
        return EntityUtil.staticPropertiesFilter(neoEdge.getAllProperties());
    }

    // NOTE!: require external synchronization
    private boolean tpExist(String key) {
        return neoEdge.hasProperty(EntityUtil.temporalPropertyWrapper(key));
    }


    /******************** Schema Change ********************/
    @Override
    public void createTemporalProperty(String key) {
        try (Lock ignored = acquireIX()) {
            if (tpExist(key)) {
                log.info("Relationship already has this temporal property.");
                throw new TemporalPropertyExistsException();
            }
            neoEdge.setProperty(EntityUtil.temporalPropertyWrapper(key), TGraphConfig.TEMPORAL_PROPERTY_VALUE_PLACEHOLDER);
        }
    }

    @Override
    public void removeTemporalProperty(String key) throws TransactionAbortException {
        try (Lock ignored = acquireIX()) {
            if (!tpExist(key)) {
                log.info("Node does not have this temporal property.");
                throw new TemporalPropertyNotExistsException();
            }
            neoEdge.removeProperty(EntityUtil.temporalPropertyWrapper(key));
            // we should remove all temporal value for consistency, thus x-lock is needed.
            acquireX(key);
            var prefix = EdgeTemporalPropertyKeyPrefix.of(startId, endId, key);
            exeCtx.getLogWb().append(LogEntry.removeEdgePrefix(prefix));
            exeCtx.getEdgeWb().removePrefix(prefix);
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }


    @Override
    public boolean hasTemporalProperty(String key) {
        try (Lock ignored = acquireIS()) {
            return tpExist(key);
        }
    }

    @Override
    public Object getTemporalPropertyValue(String key, Timestamp timestamp) throws TransactionAbortException {
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireS(key);
            return exeCtx.getEdge().get(EdgeTemporalPropertyKey.of(startId, endId, key, timestamp.getTime()));
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public List<Pair<Timestamp, Object>> getTemporalPropertyValue(String key, Timestamp start, Timestamp end) throws TransactionAbortException {
        if (start.compareTo(end) >= 0) {
            throw new IllegalArgumentException();
        }
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireS(key);
            return exeCtx.getEdge().rangeGet(EdgeTemporalPropertyKey.of(startId, endId, key, start.getTime()), EdgeTemporalPropertyKey.of(startId, endId, key, end.getTime()));
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public void setTemporalPropertyValue(String key, Timestamp timestamp, Object value) throws TransactionAbortException {
        var k = EdgeTemporalPropertyKey.of(startId, endId, key, timestamp.getTime());
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            exeCtx.getLogWb().append(LogEntry.putEdge(k, value));
            exeCtx.getEdgeWb().put(k, value);
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public void setTemporalPropertyValue(String key, Timestamp start, Timestamp end, Object value) throws TransactionAbortException {
        if (start.compareTo(end) >= 0) {
            throw new IllegalArgumentException();
        }
        var st = EdgeTemporalPropertyKey.of(startId, endId, key, start.getTime());
        var en = EdgeTemporalPropertyKey.of(startId, endId, key, end.getTime() - 1);
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            exeCtx.getLogWb().append(LogEntry.putEdge(st, value));
            exeCtx.getLogWb().append(LogEntry.putEdge(en, value));
            exeCtx.getEdgeWb().put(st, value);
            exeCtx.getEdgeWb().put(en, value);
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }


    @Override
    public void removeTemporalPropertyValue(String key, Timestamp timestamp) throws TransactionAbortException {
        var k = EdgeTemporalPropertyKey.of(startId, endId, key, timestamp.getTime());
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            exeCtx.getLogWb().append(LogEntry.removeEdge(k));
            exeCtx.getEdgeWb().remove(k);
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public void removeTemporalPropertyValue(String key, Timestamp start, Timestamp end) throws TransactionAbortException {
        if (start.compareTo(end) >= 0) {
            throw new IllegalArgumentException();
        }
        var st = EdgeTemporalPropertyKey.of(startId, endId, key, start.getTime());
        var en = EdgeTemporalPropertyKey.of(startId, endId, key, end.getTime());
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            exeCtx.getLogWb().append(LogEntry.removeEdgeRange(st, en));
            exeCtx.getEdgeWb().removeRange(st, en);
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public void removeTemporalPropertyValue(String key) throws TransactionAbortException {
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            var prefix = EdgeTemporalPropertyKeyPrefix.of(startId, endId, key);
            exeCtx.getLogWb().append(LogEntry.removeEdgePrefix(prefix));
            exeCtx.getEdgeWb().removePrefix(prefix);
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public Iterable<String> getTemporalPropertyKeys() {
        try (Lock ignored = acquireIS()) {
            return EntityUtil.temporalPropertyKesFilter(neoEdge.getPropertyKeys());
        }
    }

    @Override
    public void delete() throws TransactionAbortException {
        try (Lock ignored = acquireIX()) {
            var tps = EntityUtil.temporalPropertyKesFilter(neoEdge.getPropertyKeys());
            for (var tp : tps) {
                acquireX(tp);
                var prefix = EdgeTemporalPropertyKeyPrefix.of(startId, endId, tp);
                exeCtx.getLogWb().append(LogEntry.removeEdgePrefix(prefix));
                exeCtx.getEdgeWb().removePrefix(prefix);
            }
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public Node getStartNode() {
        var neoStartNode = neoEdge.getStartNode();
        return new Vertex(neoStartNode, exeCtx);
    }

    @Override
    public Node getEndNode() {
        var neoEndNode = neoEdge.getEndNode();
        return new Vertex(neoEndNode, exeCtx);
    }

    @Override
    public Node getOtherNode(Node node) {
        var vertex = (Vertex) node;
        var neoOtherNode = neoEdge.getOtherNode(vertex.neoVertex);
        return new Vertex(neoOtherNode, exeCtx);
    }

    @Override
    public Node[] getNodes() {
        var neoNodes = neoEdge.getNodes();
        Node[] ret = new Node[neoNodes.length];
        int ind = 0;
        for (var neoNode : neoNodes) {
            ret[ind++] = new Vertex(neoNode, exeCtx);
        }
        return ret;
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
