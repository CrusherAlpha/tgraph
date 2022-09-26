package impl.tgraphdb;

import kvstore.KVEngine;
import kvstore.RocksEngine;
import kvstore.StoreOptions;
import txn.TransactionIDGenerator;

public class TGraphMetaDB {
    private final KVEngine db;

    private final TransactionIDGenerator txnIDGenerator;

    public TGraphMetaDB(String databaseDirectory) {
        GraphSpaceID metaGraph = new GraphSpaceID(0, "meta-db");
        StoreOptions opt = StoreOptions.of(metaGraph, databaseDirectory);
        this.db = new RocksEngine(opt);
        txnIDGenerator = new TransactionIDGenerator(this.db);
    }

    long getNextTransactionID() {
        return txnIDGenerator.getNextTransactionID();
    }
}
