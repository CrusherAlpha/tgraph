package txn;

import common.Codec;
import impl.tgraphdb.GraphSpaceID;
import kvstore.KVEngine;
import kvstore.RocksEngine;
import kvstore.StoreOptions;

import java.util.ArrayList;
import java.util.List;

// false positive, transactions not in table must be committed/aborted,
// transactions in table is uncertain,
// thus we should judge if those transactions are really active(through commit log in Neo4j).

public class ActiveTransactionTable {
    private final KVEngine db;

    public ActiveTransactionTable(GraphSpaceID graph, String databaseDirectory) {
        StoreOptions opt = StoreOptions.of(graph, databaseDirectory);
        this.db = new RocksEngine(opt);
    }

    // start
    public void put(Long transactionID) {
        db.put(Codec.longToBytes(transactionID), Codec.encodeString("_"));
    }

    // commit or abort
    public void delete(Long transactionID) {
        db.remove(Codec.longToBytes(transactionID));
    }

    // for purge thread to gc
    public void multiDelete(List<Long> transactionIDs) {
        List<byte[]> keys = new ArrayList<>(transactionIDs.size());
        for (var txnID: transactionIDs) {
            keys.add(Codec.longToBytes(txnID));
        }
        db.multiRemove(keys);
    }

    // for recovery
    public List<Long> scan() {
        var all = db.scan();
        List<Long> ret = new ArrayList<>();
        for (var pr : all) {
            Long txnID = Codec.bytesToLong(pr.first());
            ret.add(txnID);
        }
        return ret;
    }

    public void stop() {
        db.stop();
    }

    void drop() {
        db.drop();
    }
}
