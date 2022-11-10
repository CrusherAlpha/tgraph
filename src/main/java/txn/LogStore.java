package txn;

import common.Codec;
import impl.tgraphdb.GraphSpaceID;
import kvstore.KVEngine;
import kvstore.RocksEngine;
import kvstore.StoreOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

// In fact, log store only need sequential write, maybe we
// can implement SequentialFile to optimize log store, but
// here we just use KVEngine for simplicity.
public class LogStore {
    private final KVEngine store;
    private final String dataPath;
    private final GraphSpaceID graph;

    private static final Log log = LogFactory.getLog(LogStore.class);

    public LogStore(GraphSpaceID graph, String dataPath) {
        this.graph = graph;
        this.dataPath = dataPath;
        StoreOptions opt = StoreOptions.of(graph, dataPath);
        this.store = new RocksEngine(opt);
    }


    public String getRoot() {
        return dataPath;
    }

    public void stop() {
        store.stop();
        log.info(String.format("Stop LogStore succeed, belongs to graph %s.", graph.getGraphName()));
    }

    public LogWriteBatch startBatchWrite() {
        return new LogWriteBatch();
    }

    public boolean commitBatchWrite(long txnID, LogWriteBatch batch) {
        return store.put(Codec.longToBytes(txnID), Codec.encodeValue(batch));
    }

    // for purge thread to gc
    public void multiDelete(List<Long> txnIDs) {
        List<byte[]> keys = new ArrayList<>(txnIDs.size());
        for (var txnID : txnIDs) {
            keys.add(Codec.longToBytes(txnID));
        }
        store.multiRemove(keys);
    }

    // used in failure recovery
    public List<LogWriteBatch> multiRead(List<Long> txnIDs) {
        List<byte[]> keys = new ArrayList<>(txnIDs.size());
        for (var txnID : txnIDs) {
            keys.add(Codec.longToBytes(txnID));
        }
        var values = store.multiGet(keys);
        List<LogWriteBatch> ret = new ArrayList<>(values.size());
        for (var value : values) {
            ret.add((LogWriteBatch) Codec.decodeValue(value));
        }
        return ret;
    }

    void drop() {
        store.drop();
    }


}
