package txn;

import common.Codec;
import kvstore.KVEngine;

import java.util.concurrent.locks.ReentrantLock;

// txnID 0, 1, 2 is reserved for internal function.
// thread safe
public class TransactionIDGenerator {
    private final KVEngine db;

    private final ReentrantLock mutex = new ReentrantLock();

    private long nextTransactionID;
    private long currentRoundTransactionIDThreshold;
    private static final long batch = 10_000;
    private static final long startTransactionID = 3;

    private static final byte[] TAG = Codec.encodeString("transaction_id");


    public TransactionIDGenerator(KVEngine db) {
        this.db = db;
        var id = db.get(TAG, null);
        nextTransactionID = id == null ? startTransactionID : Codec.bytesToLong(id);
        currentRoundTransactionIDThreshold = nextTransactionID + batch;
        db.put(TAG, Codec.longToBytes(currentRoundTransactionIDThreshold));
    }

    public long getNextTransactionID() {
        mutex.lock();
        try {
            if (nextTransactionID >= currentRoundTransactionIDThreshold) {
                currentRoundTransactionIDThreshold += batch;
                db.put(TAG, Codec.longToBytes(currentRoundTransactionIDThreshold));
            }
            return nextTransactionID++;
        } finally {
            mutex.unlock();
        }
    }
}
