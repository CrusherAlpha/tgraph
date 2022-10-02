package txn;

// Executor should surround by txn.
public class EntityExecutorContext {
    // belong to which txnID;
    private final long txnID;
    private final LockManager lockManager;
    private final LogWriteBatch logWb;

    public EntityExecutorContext(long txnID, LockManager lockManager, LogWriteBatch logWb) {
        this.txnID = txnID;
        this.lockManager = lockManager;
        this.logWb = logWb;
    }

    public long getTxnID() {
        return txnID;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public LogWriteBatch getLogWb() {
        return logWb;
    }
}
