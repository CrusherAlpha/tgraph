package cn.edu.buaa.act.tgraph.impl.tgraphdb;

import cn.edu.buaa.act.tgraph.api.tgraphdb.Node;
import cn.edu.buaa.act.tgraph.api.tgraphdb.Relationship;
import cn.edu.buaa.act.tgraph.api.tgraphdb.TemporalPropertyExistsException;
import cn.edu.buaa.act.tgraph.api.tgraphdb.TemporalPropertyNotExistsException;
import com.google.common.base.Preconditions;
import cn.edu.buaa.act.tgraph.common.EntityUtil;
import cn.edu.buaa.act.tgraph.common.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.RelationshipType;
import cn.edu.buaa.act.tgraph.property.VertexTemporalPropertyKey;
import cn.edu.buaa.act.tgraph.property.VertexTemporalPropertyKeyPrefix;
import cn.edu.buaa.act.tgraph.txn.EntityExecutorContext;
import cn.edu.buaa.act.tgraph.txn.LogEntry;
import cn.edu.buaa.act.tgraph.txn.TemporalPropertyID;
import cn.edu.buaa.act.tgraph.txn.TransactionAbortException;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// In Neo4j, Vertex/Edge is like Executor in RDMS.
// The difficulty of implementing Vertex/Edge is schema change.
// In fact, static property changed handled by Neo4j itself,
// TGraph is in charge of temporal property change, and we
// implement it through multi-level lock.
public class Vertex implements Node {

    private static final Log log = LogFactory.getLog(Vertex.class);

    public final org.neo4j.graphdb.Node neoVertex;
    private final EntityExecutorContext exeCtx;

    private final long id;

    public Vertex(org.neo4j.graphdb.Node neoVertex, EntityExecutorContext exeCtx) {
        this.neoVertex = neoVertex;
        this.exeCtx = exeCtx;
        this.id = neoVertex.getId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vertex vertex = (Vertex) o;
        return id == vertex.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    private Lock acquireIS() {
        return exeCtx.getGraphTxn().acquireReadLock(neoVertex);
    }

    private Lock acquireIX() {
        return exeCtx.getGraphTxn().acquireWriteLock(neoVertex);
    }

    private void doAcquireSX(String tp, boolean share) throws TransactionAbortException {
        var lm = exeCtx.getTxnManager().getLockManager();
        var txn = exeCtx.getTxnManager().getTransaction(exeCtx.getTxnID());
        Preconditions.checkNotNull(txn, "All operations must be surrounded by transaction.");
        try {
            if (share) {
                lm.acquireShared(txn, TemporalPropertyID.vertex(id, tp));
            } else {
                var t = TemporalPropertyID.vertex(id, tp);
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
        return neoVertex.hasProperty(key);
    }

    @Override
    public Object getProperty(String key) {
        return neoVertex.getProperty(key);
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        return neoVertex.getProperty(key, defaultValue);
    }

    @Override
    public void setProperty(String key, Object value) {
        neoVertex.setProperty(key, value);
    }

    @Override
    public Object removeProperty(String key) {
        return neoVertex.removeProperty(key);
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return EntityUtil.staticPropertyKeysFilter(neoVertex.getPropertyKeys());
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        return neoVertex.getProperties(keys);
    }

    @Override
    public Map<String, Object> getAllProperties() {
        return EntityUtil.staticPropertiesFilter(neoVertex.getAllProperties());
    }

    // NOTE!: require external synchronization
    private boolean tpExist(String key) {
        return neoVertex.hasProperty(EntityUtil.temporalPropertyWrapper(key));
    }

    /******************** Schema Change ********************/
    @Override
    public void createTemporalProperty(String key) {
        try (Lock ignored = acquireIX()) {
            if (tpExist(key)) {
                log.info("Node already has this temporal property.");
                throw new TemporalPropertyExistsException();
            }
            neoVertex.setProperty(EntityUtil.temporalPropertyWrapper(key), TGraphConfig.TEMPORAL_PROPERTY_VALUE_PLACEHOLDER);
        }
    }

    @Override
    public void removeTemporalProperty(String key) throws TransactionAbortException {
        try (Lock ignored = acquireIX()) {
            if (!tpExist(key)) {
                log.info("Node does not have this temporal property.");
                throw new TemporalPropertyNotExistsException();
            }
            neoVertex.removeProperty(EntityUtil.temporalPropertyWrapper(key));
            // we should remove all temporal value for consistency, thus x-lock is needed.
            acquireX(key);
            var prefix = VertexTemporalPropertyKeyPrefix.of(id, key);
            exeCtx.getLogWb().append(LogEntry.removeVertexPrefix(prefix));
            exeCtx.getVertexWb().removePrefix(prefix);
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
            return exeCtx.getVertex().get(VertexTemporalPropertyKey.of(id, key, timestamp.getTime()));
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
            return exeCtx.getVertex().rangeGet(VertexTemporalPropertyKey.of(id, key, start.getTime()), VertexTemporalPropertyKey.of(id, key, end.getTime()));
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public void setTemporalPropertyValue(String key, Timestamp timestamp, Object value) throws TransactionAbortException {
        var k = VertexTemporalPropertyKey.of(id, key, timestamp.getTime());
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            exeCtx.getLogWb().append(LogEntry.putVertex(k, value));
            exeCtx.getVertexWb().put(k, value);
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
        var st = VertexTemporalPropertyKey.of(id, key, start.getTime());
        var en = VertexTemporalPropertyKey.of(id, key, end.getTime() - 1);
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            exeCtx.getLogWb().append(LogEntry.putVertex(st, value));
            exeCtx.getLogWb().append(LogEntry.putVertex(en, value));
            exeCtx.getVertexWb().put(st, value);
            exeCtx.getVertexWb().put(en, value);
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }


    @Override
    public void removeTemporalPropertyValue(String key, Timestamp timestamp) throws TransactionAbortException {
        var k = VertexTemporalPropertyKey.of(id, key, timestamp.getTime());
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            exeCtx.getLogWb().append(LogEntry.removeVertex(k));
            exeCtx.getVertexWb().remove(k);
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
        var st = VertexTemporalPropertyKey.of(id, key, start.getTime());
        var en = VertexTemporalPropertyKey.of(id, key, end.getTime());
        try (Lock ignored = acquireIS()) {
            if (!tpExist(key)) {
                log.warn(String.format("temporal property %s does not exist.", key));
                throw new TemporalPropertyNotExistsException();
            }
            acquireX(key);
            exeCtx.getLogWb().append(LogEntry.removeVertexRange(st, en));
            exeCtx.getVertexWb().removeRange(st, en);
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
            var prefix = VertexTemporalPropertyKeyPrefix.of(id, key);
            exeCtx.getLogWb().append(LogEntry.removeVertexPrefix(prefix));
            exeCtx.getVertexWb().removePrefix(prefix);
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    @Override
    public Iterable<String> getTemporalPropertyKeys() {
        try (Lock ignored = acquireIS()) {
            return EntityUtil.temporalPropertyKesFilter(neoVertex.getPropertyKeys());
        }
    }

    @Override
    public void delete() throws TransactionAbortException {
        try (Lock ignored = acquireIX()) {
            var tps = EntityUtil.temporalPropertyKesFilter(neoVertex.getPropertyKeys());
            for (var tp : tps) {
                acquireX(tp);
                var prefix = VertexTemporalPropertyKeyPrefix.of(id, tp);
                exeCtx.getLogWb().append(LogEntry.removeVertexPrefix(prefix));
                exeCtx.getVertexWb().removePrefix(prefix);
            }
        } catch (TransactionAbortException e) {
            log.info(e.getInfo());
            throw e;
        }
    }

    private Iterable<Relationship> relationshipWrapper(Iterable<org.neo4j.graphdb.Relationship> neoRels) {
        var ret = new ArrayList<Relationship>();
        for (var rel : neoRels) {
            ret.add(new Edge(rel, exeCtx));
        }
        return ret;
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        var rels = neoVertex.getRelationships();
        return relationshipWrapper(rels);
    }

    @Override
    public boolean hasRelationship() {
        return neoVertex.hasRelationship();
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType... types) {
        var rels = neoVertex.getRelationships(types);
        return relationshipWrapper(rels);
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
        var rels = neoVertex.getRelationships(direction, types);
        return relationshipWrapper(rels);
    }

    @Override
    public boolean hasRelationship(RelationshipType... types) {
        return neoVertex.hasRelationship(types);
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        return neoVertex.hasRelationship(direction, types);
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction dir) {
        var rels = neoVertex.getRelationships(dir);
        return relationshipWrapper(rels);
    }

    @Override
    public boolean hasRelationship(Direction dir) {
        return neoVertex.hasRelationship(dir);
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        var rel = neoVertex.getSingleRelationship(type, dir);
        return new Edge(rel, exeCtx);
    }

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        var otherVertex = (Vertex) otherNode;
        var rel = neoVertex.createRelationshipTo(otherVertex.neoVertex, type);
        return new Edge(rel, exeCtx);
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        return neoVertex.getRelationshipTypes();
    }

    @Override
    public int getDegree() {
        return neoVertex.getDegree();
    }

    @Override
    public int getDegree(RelationshipType type) {
        return neoVertex.getDegree(type);
    }

    @Override
    public int getDegree(Direction direction) {
        return neoVertex.getDegree(direction);
    }

    @Override
    public int getDegree(RelationshipType type, Direction direction) {
        return neoVertex.getDegree(type, direction);
    }

    @Override
    public void addLabel(Label label) {
        neoVertex.addLabel(label);
    }

    @Override
    public void removeLabel(Label label) {
        neoVertex.removeLabel(label);
    }

    @Override
    public boolean hasLabel(Label label) {
        return neoVertex.hasLabel(label);
    }

    @Override
    public Iterable<Label> getLabels() {
        return neoVertex.getLabels();
    }
}
