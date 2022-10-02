package txn;


import api.tgraphdb.Transaction;
import impl.tgraphdb.ActiveTransactionTable;
import impl.tgraphdb.GraphSpaceID;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

// TransactionManager keeps track of all running transactions in this database.
// Note!: All member variables are thread safe, do not require external synchronization

public class TransactionManager {
    private AtomicLong nextTxnID = new AtomicLong(0);
    private final GraphSpaceID graphSpaceID;
    private final LockManager lockManager;
    private final LogStore logStore;
    private final ActiveTransactionTable activeTxnTable;

    private static final HashMap<Long, Transaction> txnMap = new HashMap<>();

    public TransactionManager(GraphSpaceID graph) {
        this.graphSpaceID = graph;
        this.lockManager = new LockManager();
        this.logStore = new LogStore(graph, graph.getDatabasePath() + "/log");
        this.activeTxnTable = new ActiveTransactionTable(graph, graph.getDatabasePath() + "/active-txn-table");
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public LogStore getLogStore() {
        return logStore;
    }

    // for internal usage, thus return TransactionImpl
    public static TransactionImpl getTransaction(long txnID) {
        return null;
    }

    public Transaction beginTransaction() {
        return null;
    }

    public void commitTransaction(Transaction transaction) {

    }

    public void abortTransaction(Transaction transaction) {

    }

    private void releaseLock(Transaction transaction) {

    }
}
