package impl.tgraphdb;

import common.Codec;
import kvstore.KVEngine;
import kvstore.RocksEngine;
import kvstore.StoreOptions;

import java.util.ArrayList;
import java.util.List;

// false positive, transactions not in table must be committed,
// transactions in table may be committed,
// thus we should judge if those transactions are really active.

public class ActiveTransactionTable {
    private final KVEngine db;

    public ActiveTransactionTable(GraphSpaceID graph, String databaseDirectory) {
        StoreOptions opt = StoreOptions.of(graph, databaseDirectory);
        this.db = new RocksEngine(opt);
    }

    // start
    void put(Long transactionID) {
        db.put(Codec.longToBytes(transactionID), Codec.encodeString("_"));
    }

    // commit or abort
    void delete(Long transactionID) {
        db.remove(Codec.longToBytes(transactionID));
    }

    // for recovery
    List<Long> scan() {
        var all = db.scan();
        List<Long> ret = new ArrayList<>();
        for (var pr : all) {
            Long txnID = Codec.bytesToLong(pr.first());
            ret.add(txnID);
        }
        return ret;
    }
}
