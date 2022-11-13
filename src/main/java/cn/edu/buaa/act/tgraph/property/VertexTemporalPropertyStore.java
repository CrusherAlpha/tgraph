package cn.edu.buaa.act.tgraph.property;

import com.google.common.base.Preconditions;
import cn.edu.buaa.act.tgraph.common.Codec;
import cn.edu.buaa.act.tgraph.common.Pair;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.GraphSpaceID;
import cn.edu.buaa.act.tgraph.kvstore.Comparator;
import cn.edu.buaa.act.tgraph.kvstore.KVEngine;
import cn.edu.buaa.act.tgraph.kvstore.RocksEngine;
import cn.edu.buaa.act.tgraph.kvstore.StoreOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDB;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


// NOTE!: get everything in memory may cause OOM.
// TODO(crusher): fix it use PropertyIter

// Every write will be in memory until Commit.
// Read will go into underlying store.
// And for Neo4j semantic compatibility, write should not be read until commit,
// even this write is from yours.

// Vertex and Edge may have the different behavior and inheritance is disgusting.
public class VertexTemporalPropertyStore {
    private final GraphSpaceID graph;
    private final KVEngine store;
    private final String dataPath;

    private static final Log log = LogFactory.getLog(VertexTemporalPropertyStore.class);

    static {
        RocksDB.loadLibrary();
    }

    // the #argument is fine, we don't encapsulate these in StoreOption
    public VertexTemporalPropertyStore(GraphSpaceID graph, String dataPath, boolean readonly) {
        this.graph = graph;
        this.dataPath = dataPath;
        var comparator = new VertexTemporalPropertyKeyComparator(new ComparatorOptions());
        StoreOptions opt = StoreOptions.of(this.graph, this.dataPath, readonly, new Comparator(comparator));
        store = new RocksEngine(opt);
    }

    public String getRoot() {
        return dataPath;
    }

    public void stop() {
        store.stop();
        log.info(String.format("Stop VertexTemporalPropertyStore succeed, belongs to graph %s.", graph.getGraphName()));
    }

    // We only expose write batch interface, write will go into the batch.
    // In fact, write batch is held by txn.
    public VertexTemporalPropertyWriteBatch startBatchWrite() {
        var wb = store.startBatchWrite();
        return new VertexTemporalPropertyWriteBatch(wb, store);
    }

    public boolean commitBatchWrite(VertexTemporalPropertyWriteBatch batch, boolean disableWAL, boolean sync, boolean wait) {
        return store.commitBatchWrite(batch.getWb(), disableWAL, sync, wait);
    }

    // TimePoint Get
    // Return the value of the max timestamp which <= key.timestamp
    public Object get(VertexTemporalPropertyKey key) {
        var prefix = key.getPrefix();
        var pr = store.getForPrev(key.toBytes(), null);
        if (pr == null) {
            return null;
        }
        var realKey = VertexTemporalPropertyKey.fromBytes(pr.first());
        return realKey.getPrefix().equals(prefix) ? Codec.decodeValue(pr.second()) : null;
    }

    // Batch TimePoint Get
    // Return the value of the max timestamp which <= key.timestamp
    public List<Object> multiGet(List<VertexTemporalPropertyKey> keys) {
        List<byte[]> k = new ArrayList<>(keys.size());
        for (VertexTemporalPropertyKey key: keys) {
            k.add(key.toBytes());
        }
        var r = store.multiGetForPrev(k);
        Preconditions.checkState(keys.size() == r.size(), "MultiGet keys and return values are not consistent");
        List<Object> ret = new ArrayList<>(r.size());
        int ind = 0;
        for (var v : r) {
            if (v == null) {
                ret.add(null);
                continue;
            }
            var realKey = VertexTemporalPropertyKey.fromBytes(v.first());
            ret.add(realKey.getPrefix().equals(keys.get(ind).getPrefix()) ? Codec.decodeValue(v.second()) : null);
            ++ind;
        }
        return ret.isEmpty() ? null : ret;
    }

    // TimeRange Get
    public List<Pair<Timestamp, Object>> rangeGet(VertexTemporalPropertyKey start, VertexTemporalPropertyKey end) {
        Preconditions.checkState(start.getPrefix().equals(end.getPrefix()), "start and end should have the same prefix");
        List<Pair<Timestamp, Object>> ret = new ArrayList<>();
        try (var iter = store.rangePrev(start.toBytes(), end.toBytes())) {
            while (iter.valid()) {
                var key = VertexTemporalPropertyKey.fromBytes(iter.key());
                Timestamp timestamp = new Timestamp(key.getTimestamp());
                ret.add(Pair.of(timestamp, Codec.decodeValue(iter.value())));
                iter.next();
            }
        }
        return ret.isEmpty() ? null : ret;
    }

    // TimeRange with Prefix Get
    public List<Pair<Timestamp, Object>> rangeWithPrefixGet(VertexTemporalPropertyKey start) {
        List<Pair<Timestamp, Object>> ret = new ArrayList<>();
        try (var iter = store.rangePrevWithPrefix(start.toBytes(), start.getPrefix().toBytes())) {
            while (iter.valid()) {
                var key = VertexTemporalPropertyKey.fromBytes(iter.key());
                Timestamp timestamp = new Timestamp(key.getTimestamp());
                ret.add(Pair.of(timestamp, Codec.decodeValue(iter.value())));
                iter.next();
            }
        }
        return ret.isEmpty() ? null : ret;
    }

    // Prefix Get
    public List<Pair<Timestamp, Object>> prefixGet(VertexTemporalPropertyKeyPrefix prefix) {
        List<Pair<Timestamp, Object>> ret = new ArrayList<>();
        try (var iter = store.prefix(prefix.toBytes(), null)) {
            while (iter.valid()) {
                var key = VertexTemporalPropertyKey.fromBytes(iter.key());
                Timestamp timestamp = new Timestamp(key.getTimestamp());
                ret.add(Pair.of(timestamp, Codec.decodeValue(iter.value())));
                iter.next();
            }
        }
        return ret.isEmpty() ? null : ret;
    }

    public boolean flush() {
        return store.flush();
    }

    public void drop() {
        store.drop();
    }

}
