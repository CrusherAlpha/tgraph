package txn;


enum AbortReason {
    UPGRADE_CONFLICT,
    DEADLOCK,
}

public class TransactionAbortException extends Exception {
    private final long txnID;
    private final AbortReason abortReason;

    public TransactionAbortException(long txnID, AbortReason abortReason) {
        this.txnID = txnID;
        this.abortReason = abortReason;
    }

    public long getTxnID() {
        return txnID;
    }

    public AbortReason getAbortReason() {
        return abortReason;
    }

    public String getInfo() {
        switch (abortReason) {
            case UPGRADE_CONFLICT: {
                return String.format("Transaction %d aborted because another transaction is already waiting to upgrade its lock\n", txnID);
            }
            case DEADLOCK: {
                return String.format("Transaction %d aborted on deadlock\n", txnID);
            }
        }
        return "";
    }


}
