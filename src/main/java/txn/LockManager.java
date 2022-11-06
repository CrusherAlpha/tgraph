package txn;

import impl.tgraphdb.TGraphConfig;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

enum LockMode {
    SHARED, EXCLUSIVE
}

class LockRequest {
    public final long txnID;
    public LockMode lockMode;
    public boolean granted = false;


    public LockRequest(long txnID, LockMode lockMode) {
        this.txnID = txnID;
        this.lockMode = lockMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockRequest that = (LockRequest) o;
        return txnID == that.txnID;
    }

    @Override
    public int hashCode() {
        return Objects.hash(txnID);
    }

    @Override
    public String toString() {
        return "LockRequest{" +
                "txnID=" + txnID +
                ", lockMode=" + lockMode +
                ", granted=" + granted +
                '}';
    }
}

class LockRequestQueue {
    public List<LockRequest> requestQueue = new ArrayList<>(); // guard by mu
    public final ReentrantLock mu = new ReentrantLock();
    // for notifying all waiting transactions.
    public final Condition cv = mu.newCondition();
    // for lock upgrade.
    public boolean upgrading = false;
}

// NOTE!: acquire lock may cause TransactionAbortException,
// caller is in charge of the release lock.
public class LockManager implements AutoCloseable {

    private static final Log log = LogFactory.getLog(LockManager.class);

    // hold running transaction map reference passed by TransactionManager.
    private final ConcurrentHashMap<Long, TransactionImpl> txnMap;

    public final ReentrantLock mu = new ReentrantLock();
    // entity identifier + property name ->  lock request queue
    private final HashMap<TemporalPropertyID, LockRequestQueue> lockTable = new HashMap<>(); // guarded by mu.

    // for deadlock detection
    private final static int SHUTDOWN_TIME = 2;
    private final HashMap<Long, List<Long>> waitFor = new HashMap<>();
    ScheduledExecutorService deadlockExe = Executors.newSingleThreadScheduledExecutor();

    public LockManager(ConcurrentHashMap<Long, TransactionImpl> txnMap) {
        this.txnMap = txnMap;
        deadlockExe.scheduleAtFixedRate(this::runCycleDetection, TGraphConfig.DEADLOCK_DETECT_INTERVAL, TGraphConfig.DEADLOCK_DETECT_INTERVAL, TimeUnit.SECONDS);
    }

    // NOTE!: this api is exposed only for LockManager ut, you should not disable deadlock detection.
    public LockManager(ConcurrentHashMap<Long, TransactionImpl> txnMap, boolean enableDeadlockDetection) {
        log.info("NOTE!: you create a lock manager using a test only api.");
        this.txnMap = txnMap;
        if (enableDeadlockDetection) {
            deadlockExe.scheduleAtFixedRate(this::runCycleDetection, TGraphConfig.DEADLOCK_DETECT_INTERVAL, TGraphConfig.DEADLOCK_DETECT_INTERVAL, TimeUnit.SECONDS);
        }
    }

    private void addEdge(long from, long to) {
        // add from node if absent
        var fromEdges = waitFor.computeIfAbsent(from, k -> new ArrayList<>());
        // add to node if absent
        waitFor.computeIfAbsent(to, k -> new ArrayList<>());
        if (fromEdges.contains(to)) {
            return;
        }
        fromEdges.add(to);
    }

    private boolean dfs(long cur, HashMap<Long, Boolean> visited, HashMap<Long, Boolean> mem) {
        if (mem.containsKey(cur)) {
            return false;
        }
        if (visited.containsKey(cur)) {
            return true;
        }
        visited.put(cur, true);
        var ends = waitFor.get(cur);
        Preconditions.checkNotNull(ends);
        for (var to : ends) {
            if (dfs(to, visited, mem)) {
                return true;
            }
        }
        return false;
    }

    // NOTE!: require external synchronization
    // pick a victim random.
    private Optional<Long> pickVictim() {
        HashMap<Long, Boolean> visited = new HashMap<>();
        HashMap<Long, Boolean> mem = new HashMap<>();
        var txns = waitFor.keySet();
        for (var txn : txns) {
            visited.clear();
            if (dfs(txn, visited, mem)) {
                return Optional.of(txn);
            }
            mem.put(txn, true);
        }
        return Optional.empty();
    }

    // NOTE!: require external synchronization
    private void buildWaitForGraph() {
        waitFor.clear();
        List<Long> hold = new ArrayList<>();
        List<Long> wait = new ArrayList<>();
        for (var lockTableEntry : lockTable.entrySet()) {
            var lq = lockTableEntry.getValue();
            hold.clear();
            wait.clear();
            lq.mu.lock();
            for (var lr : lq.requestQueue) {
                var txn = txnMap.get(lr.txnID);
                if (txn == null || txn.getState() == TransactionState.ABORTED) {
                    continue;
                }
                if (lr.granted) {
                    hold.add(lr.txnID);
                } else {
                    wait.add(lr.txnID);
                }
            }
            lq.mu.unlock();
            for (var w : wait) {
                for (var h : hold) {
                    addEdge(w, h);
                }
            }
        }
    }

    // detect deadlock through wait-for graph, judge if exists cycle in graph
    private void runCycleDetection() {
        try {
            mu.lock();
            log.info("starting deadlock detection.");
            buildWaitForGraph();
            // pick victims until no cycle
            boolean deadlock = false;
            for (var victim = pickVictim(); victim != null && victim.isPresent(); victim = pickVictim()) {
                long victimTxnID = victim.get();
                deadlock = true;
                log.info(String.format("deadlock occurs, victim txn id: %d.", victimTxnID));

                // abort the victim
                var txn = txnMap.get(victimTxnID);
                Preconditions.checkNotNull(txn);
                txn.setState(TransactionState.ABORTED);

                // notify the transactions waiting for me
                HashSet<TemporalPropertyID> lockSet = new HashSet<>(txn.getSharedLockSet());
                lockSet.addAll(txn.getExclusiveLockSet());
                for (var tpID : lockSet) {
                    var lq = lockTable.get(tpID);
                    if (lq != null) {
                        try {
                            lq.mu.lock();
                            lq.cv.signalAll();
                        } finally {
                            lq.mu.unlock();
                        }
                    }
                }
                // re-build wait for graph.
                buildWaitForGraph();
            }
            if (!deadlock) {
                log.info("no deadlock occurs.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mu.unlock();
        }
    }

    // NOTE!: require external synchronization.
    private LockRequestQueue getOrCreateLockRequestQueue(TemporalPropertyID tp) {
        var lq = lockTable.get(tp);
        if (lq == null) {
            lq = new LockRequestQueue();
            lockTable.put(tp, lq);
            return lq;
        }
        return lq;
    }

    private void waitForLockCompatibleOrDeadLock(LockRequestQueue que, LockRequest lr, TransactionImpl txn) {
        while (!isLockCompatible(que, lr) && txn.getState() != TransactionState.ABORTED) {
            try {
                que.cv.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean doAcquire(TransactionImpl txn, TemporalPropertyID tp, LockMode mode) throws TransactionAbortException {

        if (mode == LockMode.SHARED) {
            if (txn.holdSLock(tp) || txn.holdXLock(tp)) {
                return true;
            }
        } else {
            if (txn.holdXLock(tp)) {
                return true;
            }
            // leave lock upgrade to upper layer.
            Preconditions.checkState(!txn.holdSLock(tp));
        }
        mu.lock();
        var lq = getOrCreateLockRequestQueue(tp);
        mu.unlock();

        LockRequest lr = new LockRequest(txn.getTxnID(), mode);

        try {
            lq.mu.lock();

            // add lr(un-granted) to lq.
            lq.requestQueue.add(lr);
            waitForLockCompatibleOrDeadLock(lq, lr, txn);

            // deadlock occurs.
            if (txn.getState() == TransactionState.ABORTED) {
                // remove lr(un-granted) from lq.
                lq.requestQueue.remove(lr);
                abortInternal(txn, AbortReason.DEADLOCK);
                return false;
            }

            // acquire Lock successfully
            lr.granted = true;
            if (mode == LockMode.SHARED) {
                txn.getSharedLockSet().add(tp);
            } else {
                txn.getExclusiveLockSet().add(tp);
            }

            return true;

        } finally {
            lq.mu.unlock();
        }
    }

    private boolean doUpgrade(TransactionImpl txn, TemporalPropertyID tp) throws TransactionAbortException {
        mu.lock();
        var lq = getOrCreateLockRequestQueue(tp);
        mu.unlock();
        try {
            lq.mu.lock();
            if (lq.upgrading) {
                abortInternal(txn, AbortReason.UPGRADE_CONFLICT);
                return false;
            }
            lq.upgrading = true;
            var ind = lq.requestQueue.indexOf(new LockRequest(txn.getTxnID(), LockMode.SHARED));
            Preconditions.checkState(ind != -1, "do not hold any lock when upgrading.");
            var lr = lq.requestQueue.get(ind);
            Preconditions.checkState(lr.granted, "lock request has not be granted");
            Preconditions.checkState(lr.lockMode == LockMode.SHARED, "lock request should be S-lock");

            Preconditions.checkState(txn.holdSLock(tp), "txn should hold S-lock.");
            Preconditions.checkState(!txn.holdXLock(tp), "txn should not hold X-lock.");

            lr.granted = false;
            lr.lockMode = LockMode.EXCLUSIVE;

            // wait for lock compatible or deadlock
            waitForLockCompatibleOrDeadLock(lq, lr, txn);

            // deadlock occurs
            if (txn.getState() == TransactionState.ABORTED) {
                txn.getSharedLockSet().remove(tp);
                lq.requestQueue.remove(lr);
                abortInternal(txn, AbortReason.DEADLOCK);
                return false;
            }

            // acquire X-Lock successfully
            lr.granted = true;
            txn.getSharedLockSet().remove(tp);
            txn.getExclusiveLockSet().add(tp);
            lq.upgrading = false;
            return true;

        } finally {
            lq.mu.unlock();
        }
    }

    public boolean acquireShared(TransactionImpl txn, TemporalPropertyID tp) throws TransactionAbortException {
        return doAcquire(txn, tp, LockMode.SHARED);
    }

    public boolean acquireExclusive(TransactionImpl txn, TemporalPropertyID tp) throws TransactionAbortException {
        return doAcquire(txn, tp, LockMode.EXCLUSIVE);
    }

    // for S-Lock -> X-Lock
    public boolean upgrade(TransactionImpl txn, TemporalPropertyID tp) throws TransactionAbortException {
        return doUpgrade(txn, tp);
    }

    // NOTE!: require external synchronization.
    private boolean doUnlock(TransactionImpl txn, TemporalPropertyID tp) {
        Preconditions.checkState(txn.getState() == TransactionState.COMMITTED, "SS2PL requires unlock occurs in commit phase");

        var lq = getOrCreateLockRequestQueue(tp);

        try {
            lq.mu.lock();
            var ind = lq.requestQueue.indexOf(new LockRequest(txn.getTxnID(), null));
            Preconditions.checkState(ind != -1, "do not hold any lock when unlock.");

            // remove myself and notify all waiting transactions
            lq.requestQueue.remove(ind);
            if (!lq.requestQueue.isEmpty()) {
                lq.cv.signalAll();
            }

            txn.getSharedLockSet().remove(tp);
            txn.getExclusiveLockSet().remove(tp);

            // garbage collection
            // if no waiting transactions, this tp should be gc to avoid OOM.
            if (lq.requestQueue.isEmpty()) {
                lockTable.remove(tp);
            }
            return true;

        } finally {
            lq.mu.unlock();
        }
    }

    // NOTE!: require external synchronization.
    // guarded by mu, invoked by upper layer.
    public boolean unlock(TransactionImpl txn, TemporalPropertyID tp) throws TransactionAbortException {
        return doUnlock(txn, tp);
    }


    // return true iff:
    // 1. que is empty
    // 2. compatible with all locks that are currently held by active txns.
    private boolean isLockCompatible(LockRequestQueue que, LockRequest req) {
        Preconditions.checkNotNull(que);
        Preconditions.checkNotNull(req);
        for (var r : que.requestQueue) {
            var txn = txnMap.get(r.txnID);
            Preconditions.checkState(txn != null);
            if (r.granted && txn.getState() == TransactionState.ACTIVE) {
                Preconditions.checkState(r.txnID != req.txnID);
                var compatible = (r.lockMode != LockMode.EXCLUSIVE && req.lockMode != LockMode.EXCLUSIVE);
                if (!compatible) {
                    return false;
                }
            }
        }
        return true;
    }

    // throws abort exception to upper layer and let upper layer release all locks.
    private static void abortInternal(TransactionImpl txn, AbortReason reason) throws TransactionAbortException {
        txn.setState(TransactionState.ABORTED);
        throw new TransactionAbortException(txn.getTxnID(), reason);
    }

    @Override
    public void close() throws Exception {
        deadlockExe.shutdown();
        if (!deadlockExe.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
            log.info(String.format("Deadlock detector did not terminate in %d second(s).", SHUTDOWN_TIME));
            deadlockExe.shutdownNow();
        }
    }
}
