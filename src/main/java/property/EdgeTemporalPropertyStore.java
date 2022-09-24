package property;

import com.google.common.base.Preconditions;
import common.Codec;
import common.Pair;
import impl.tgraphdb.GraphSpaceID;
import kvstore.Comparator;
import kvstore.KVEngine;
import kvstore.RocksEngine;
import kvstore.StoreOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDB;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

// Every write will be in memory until Commit.
// Read will go into underlying store.
// And for Neo4j semantic compatibility, write should not be read until commit,
// even this write is from yours.
public class EdgeTemporalPropertyStore {
    private final GraphSpaceID graph;
    private final KVEngine store;
    private final String dataPath;

    private final Log log = LogFactory.getLog(EdgeTemporalPropertyStore.class);

    static {
        RocksDB.loadLibrary();
    }

    // the #argument is fine, we don't encapsulate these in StoreOption
    public EdgeTemporalPropertyStore(GraphSpaceID graph, String dataPath, boolean readonly) {
        this.graph = graph;
        this.dataPath = dataPath + "/edge";
        var comparator = new EdgeTemporalPropertyKeyComparator(new ComparatorOptions());
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
    public EdgeTemporalPropertyWriteBatch startBatchWrite() {
        var wb = store.startBatchWrite();
        return new EdgeTemporalPropertyWriteBatch(wb, store);
    }

    public boolean commitBatchWrite(EdgeTemporalPropertyWriteBatch batch, boolean disableWAL, boolean sync, boolean wait) {
        return store.commitBatchWrite(batch.getWb(), disableWAL, sync, wait);
    }

    // TimePoint Get
    // Return the value of the max timestamp which <= key.timestamp
    public Object get(EdgeTemporalPropertyKey key) {
        var prefix = key.getPrefix();
        var pr = store.getForPrev(key.toBytes(), null);
        if (pr == null) {
            return null;
        }
        var realKey = EdgeTemporalPropertyKey.fromBytes(pr.first());
        return realKey.getPrefix().equals(prefix) ? Codec.decodeValue(pr.second()) : null;
    }

    // Batch TimePoint Get
    // Return the value of the max timestamp which <= key.timestamp
    public List<Object> multiGet(List<EdgeTemporalPropertyKey> keys) {
        List<byte[]> k = new ArrayList<>(keys.size());
        for (EdgeTemporalPropertyKey key: keys) {
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
            var realKey = EdgeTemporalPropertyKey.fromBytes(v.first());
            ret.add(realKey.getPrefix().equals(keys.get(ind).getPrefix()) ? Codec.decodeValue(v.second()) : null);
            ++ind;
        }
        return ret.isEmpty() ? null : ret;
    }

    // TimeRange Get
    public List<Pair<Timestamp, Object>> rangeGet(EdgeTemporalPropertyKey start, EdgeTemporalPropertyKey end) {
        Preconditions.checkState(start.getPrefix().equals(end.getPrefix()), "start and end should have the same prefix");
        List<Pair<Timestamp, Object>> ret = new ArrayList<>();
        try (var iter = store.rangePrev(start.toBytes(), end.toBytes())) {
            while (iter.valid()) {
                var key = EdgeTemporalPropertyKey.fromBytes(iter.key());
                Timestamp timestamp = new Timestamp(key.getTimestamp());
                ret.add(Pair.of(timestamp, Codec.decodeValue(iter.value())));
                iter.next();
            }
        }
        return ret.isEmpty() ? null : ret;
    }

    // TimeRange with Prefix Get
    public List<Pair<Timestamp, Object>> rangeWithPrefixGet(EdgeTemporalPropertyKey start) {
        List<Pair<Timestamp, Object>> ret = new ArrayList<>();
        try (var iter = store.rangePrevWithPrefix(start.toBytes(), start.getPrefix().toBytes())) {
            while (iter.valid()) {
                var key = EdgeTemporalPropertyKey.fromBytes(iter.key());
                Timestamp timestamp = new Timestamp(key.getTimestamp());
                ret.add(Pair.of(timestamp, Codec.decodeValue(iter.value())));
                iter.next();
            }
        }
        return ret.isEmpty() ? null : ret;
    }

    // Prefix Get
    public List<Pair<Timestamp, Object>> prefixGet(EdgeTemporalPropertyKeyPrefix prefix) {
        List<Pair<Timestamp, Object>> ret = new ArrayList<>();
        try (var iter = store.prefix(prefix.toBytes(), null)) {
            while (iter.valid()) {
                var key = EdgeTemporalPropertyKey.fromBytes(iter.key());
                Timestamp timestamp = new Timestamp(key.getTimestamp());
                ret.add(Pair.of(timestamp, Codec.decodeValue(iter.value())));
                iter.next();
            }
        }
        return ret.isEmpty() ? null : ret;
    }

    boolean flush() {
        return store.flush();
    }


}
