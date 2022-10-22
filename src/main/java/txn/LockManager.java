package txn;

import impl.tgraphdb.TGraphConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import common.Pair;
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
}

class LockRequestQueue {
    public List<LockRequest> requestQueue = new ArrayList<>(); // guard by mu
    public final ReentrantLock mu = new ReentrantLock();
    // for notifying all waiting transactions.
    public final Condition cv = mu.newCondition();
    // for lock upgrade.
    public boolean upgrading = false;
}

// For avoiding concurrent delete entity/entity temporal property and access entity temporal property time point value,
// we should acquire S-Lock first on Neo4j Entity for temporal property access,
// and acquire X-Lock first on Neo4j Entity for entity/entity tp delete.
// leave this operation to entity.
public class LockManager implements AutoCloseable {

    private static final Log log = LogFactory.getLog(LockManager.class);

    // hold running transaction map reference passed by TransactionManager.
    private final ConcurrentHashMap<Long, TransactionImpl> txnMap;

    private final ReentrantLock mu = new ReentrantLock();
    // entity identifier + property name ->  the list of <timestamp, lock request queue>
    private final HashMap<TemporalPropertyID, ArrayList<Pair<Long, LockRequestQueue>>> lockTable = new HashMap<>(); // guarded by mu

    // for deadlock detection
    private final static int SHUTDOWN_TIME = 2;
    private final HashMap<Long, List<Long>> waitFor = new HashMap<>();
    ScheduledExecutorService deadlockExe = Executors.newSingleThreadScheduledExecutor();

    public LockManager(ConcurrentHashMap<Long, TransactionImpl> txnMap) {
        this.txnMap = txnMap;
        deadlockExe.scheduleAtFixedRate(this::runCycleDetection, TGraphConfig.DEADLOCK_DETECT_INTERNAL, TGraphConfig.DEADLOCK_DETECT_INTERNAL, TimeUnit.SECONDS);
    }

    private static void doAddEdge(long from, long to, HashMap<Long, List<Long>> graph) {
        var l = graph.computeIfAbsent(from, k -> new ArrayList<>());
        var ind = Collections.binarySearch(l, to);
        // already in
        if (ind >= 0) {
            return;
        }
        l.add(-(ind + 1), to);
    }

    private static void doRemoveEdge(long from, long to, HashMap<Long, List<Long>> graph) {
        var l = graph.get(from);
        if (l != null) {
            var ind = Collections.binarySearch(l, to);
            if (ind >= 0) {
                l.remove(ind);
            }
        }

    }

    private void addEdge(long from, long to) {
        // from -> to
        doAddEdge(from, to, waitFor);
    }

    private void removeEdge(long from, long to) {
        // from -> to
        doRemoveEdge(from, to, waitFor);
    }

    private boolean dfs(long cur, HashMap<Long, Boolean> visited) {
        if (visited.containsKey(cur)) {
            return true;
        }
        visited.put(cur, true);
        for (var to : waitFor.get(cur)) {
            if (dfs(to, visited)) {
                return true;
            }
        }
        return false;
    }

    // Note!: require external synchronization
    // pick a victim random.
    private Optional<Long> pickVictim() {
        HashMap<Long, Boolean> visited = new HashMap<>();
        for (var txn : waitFor.keySet()) {
            if (dfs(txn, visited)) {
                return Optional.of(txn);
            }
        }
        return Optional.empty();
    }

    // Note!: require external synchronization
    private void buildWaitForGraph() {
        for (var lockTableEntry : lockTable.entrySet()) {
            var timeList = lockTableEntry.getValue();
            for (var pr : timeList) {
                List<Long> hold = new ArrayList<>();
                List<Long> wait = new ArrayList<>();
                var que = pr.second();
                for (var req : que.requestQueue) {
                    var txn = txnMap.get(req.txnID);
                    if (txn == null || txn.getState() == TransactionState.ABORTED) {
                        continue;
                    }
                    if (req.granted) {
                        hold.add(req.txnID);
                    } else {
                        wait.add(req.txnID);
                    }
                }
                for (var w : wait) {
                    for (var h : hold) {
                        addEdge(w, h);
                    }
                }
            }
        }
    }

    // detect deadlock through wait-for graph, judge if exists cycle in graph
    private void runCycleDetection() {
        try {
            mu.lock();
            waitFor.clear();
            buildWaitForGraph();
            // pick victims until no cycle
            for (var victim = pickVictim(); victim != null && victim.isPresent(); victim = pickVictim()) {

                // remove edge in graph
                for (var to : waitFor.get(victim.get())) {
                    removeEdge(victim.get(), to);
                }

                // abort the victim
                var txn = txnMap.get(victim.get());
                Preconditions.checkNotNull(txn);
                txn.setState(TransactionState.ABORTED);

                // notify the transactions waiting for me
                HashSet<TimePointTemporalPropertyID> lockSet = new HashSet<>(txn.getSharedLockSet());
                lockSet.addAll(txn.getExclusiveLockSet());
                for (var entry : lockSet) {
                    var l = lockTable.get(entry.getTp());
                    if (l != null) {
                        Pair<Long, LockRequestQueue> p = null;
                        // linear search here is ok, but can be optimized through binary search.
                        for (var pr : l) {
                            if (entry.getTimestamp() == pr.first()) {
                                p = pr;
                                break;
                            }
                        }
                        if (p != null) {
                            p.second().cv.notifyAll();
                        }
                    }
                }
            }

        } finally {
            mu.unlock();
        }
    }


    // Note!: require external synchronization.
    private Pair<Long, LockRequestQueue> getLockPair(TemporalPropertyID id, long timestamp) {
        var l = lockTable.get(id);
        if (l == null) {
            l = new ArrayList<>();
            LockRequestQueue que = new LockRequestQueue();
            l.add(Pair.of(timestamp, que));
            lockTable.put(id, l);
            return Pair.of(timestamp, que);
        }
        Long retT;
        LockRequestQueue retQ;
        Preconditions.checkNotNull(l);
        Pair<Long, LockRequestQueue> key = Pair.of(timestamp, null);
        var com = new Comparator<Pair<Long, LockRequestQueue>>() {
            @Override
            public int compare(Pair<Long, LockRequestQueue> o1, Pair<Long, LockRequestQueue> o2) {
                if (Objects.equals(o1.first(), o2.first())) {
                    return 0;
                }
                return o1.first() < o2.first() ? -1 : 1;
            }
        };
        // find the max one which <= the given key through binary search.
        // maybe there is a std lib, but I did not get the best practice.
        // for example, 1, 3, 5 is in list now,
        // user wants to read value in timestamp 4, so the 3 is expected,
        // we should add lock on 3.
        int low = 0;
        int high = l.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            var midVal = l.get(mid);
            int c = com.compare(midVal, key);
            if (c == 0) {
                high = mid;
                break;
            } else if (c < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        // high may be -1, means all element in list is greater than key
        Pair<Long, LockRequestQueue> pr;
        if (high == -1) {
            pr = Pair.of(timestamp, new LockRequestQueue());
            l.add(0, pr);
            retT = timestamp;
        } else {
            pr = l.get(high);
            retT = pr.first();
        }
        retQ = pr.second();
        return Pair.of(retT, retQ);
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

    private boolean doAcquire(TransactionImpl txn, TemporalPropertyID tp, long timestamp, LockMode mode) throws TransactionAbortException {

        var tpT = TimePointTemporalPropertyID.of(tp, timestamp);
        if (mode == LockMode.SHARED) {
            if (txn.holdSLock(tpT) || txn.holdXLock(tpT)) {
                return true;
            }
        } else {
            if (txn.holdXLock(tpT)) {
                return true;
            }
            // leave lock upgrade to upper layer.
            Preconditions.checkState(!txn.holdSLock(tpT));
        }
        mu.lock();
        var pr = getLockPair(tp, timestamp);
        mu.unlock();
        var t = pr.first();
        var que = pr.second();
        tpT = TimePointTemporalPropertyID.of(tp, t);

        LockRequest lr = new LockRequest(txn.getTxnID(), mode);

        try {
            que.mu.lock();

            que.requestQueue.add(lr);

            waitForLockCompatibleOrDeadLock(que, lr, txn);

            // deadlock occurs
            if (txn.getState() == TransactionState.ABORTED) {
                abortInternal(txn, AbortReason.DEADLOCK);
                return false;
            }

            // acquire Lock successfully
            lr.granted = true;
            if (mode == LockMode.SHARED) {
                txn.getSharedLockSet().add(tpT);
            } else {
                txn.getExclusiveLockSet().add(tpT);
            }

            return true;

        } finally {
            que.mu.unlock();
        }
    }

    private boolean doUpgrade(TransactionImpl txn, TemporalPropertyID tp, long timestamp) throws TransactionAbortException {
        mu.lock();
        var pr = getLockPair(tp, timestamp);
        mu.unlock();
        var t = pr.first();
        var que = pr.second();
        var tpT = TimePointTemporalPropertyID.of(tp, t);
        try {
            que.mu.lock();
            if (que.upgrading) {
                abortInternal(txn, AbortReason.UPGRADE_CONFLICT);
                return false;
            }
            que.upgrading = true;
            var ind = que.requestQueue.indexOf(new LockRequest(txn.getTxnID(), LockMode.SHARED));
            Preconditions.checkState(ind != -1, "do not hold any lock when upgrading.");
            var lr = que.requestQueue.get(ind);
            Preconditions.checkState(lr.granted, "lock request has not be granted");
            Preconditions.checkState(lr.lockMode == LockMode.SHARED, "lock request should be S-lock");

            Preconditions.checkState(txn.holdSLock(tpT), "txn should hold S-lock.");
            Preconditions.checkState(!txn.holdXLock(tpT), "txn should not hold X-lock.");

            lr.granted = false;
            lr.lockMode = LockMode.EXCLUSIVE;

            // wait for lock compatible or deadlock
            waitForLockCompatibleOrDeadLock(que, lr, txn);

            // deadlock occurs
            if (txn.getState() == TransactionState.ABORTED) {
                abortInternal(txn, AbortReason.DEADLOCK);
                return false;
            }

            // acquire X-Lock successfully
            lr.granted = true;
            txn.getSharedLockSet().remove(tpT);
            txn.getExclusiveLockSet().add(tpT);
            que.upgrading = false;
            return true;

        } finally {
            que.mu.unlock();
        }
    }

    public boolean acquireShared(TransactionImpl txn, long vertexID, String propertyName, long timestamp) throws TransactionAbortException {
        var tp = TemporalPropertyID.vertex(vertexID, propertyName);
        return doAcquire(txn, tp, timestamp, LockMode.SHARED);
    }

    public boolean acquireShared(TransactionImpl txn, long startID, long endID, String propertyName, long timestamp) throws TransactionAbortException {
        var tp = TemporalPropertyID.edge(startID, endID, propertyName);
        return doAcquire(txn, tp, timestamp, LockMode.SHARED);
    }

    public boolean acquireExclusive(TransactionImpl txn, long vertexID, String propertyName, long timestamp) throws TransactionAbortException {
        var tp = TemporalPropertyID.vertex(vertexID, propertyName);
        return doAcquire(txn, tp, timestamp, LockMode.EXCLUSIVE);
    }

    public boolean acquireExclusive(TransactionImpl txn, long startID, long endID, String propertyName, long timestamp) throws TransactionAbortException {
        var tp = TemporalPropertyID.edge(startID, endID, propertyName);
        return doAcquire(txn, tp, timestamp, LockMode.EXCLUSIVE);
    }

    // for S-Lock -> X-Lock
    public boolean upgrade(TransactionImpl txn, long vertexID, String propertyName, long timestamp) throws TransactionAbortException {
        var tp = TemporalPropertyID.vertex(vertexID, propertyName);
        return doUpgrade(txn, tp, timestamp);
    }

    // for S-Lock -> X-Lock
    public boolean upgrade(TransactionImpl txn, long startID, long endID, String propertyName, long timestamp) throws TransactionAbortException {
        var tp = TemporalPropertyID.edge(startID, endID, propertyName);
        return doUpgrade(txn, tp, timestamp);
    }

    // Note!: require external
    // guarded by mu, invoked by transaction manager.
    private boolean doUnlock(TransactionImpl txn, TemporalPropertyID tp, long timestamp) {
        Preconditions.checkState(txn.getState() == TransactionState.COMMITTED, "SS2PL requires unlock occurs in commit moment");

        var pr = getLockPair(tp, timestamp);
        var t = pr.first();
        var que = pr.second();

        var tpT = TimePointTemporalPropertyID.of(tp, t);

        try {
            que.mu.lock();
            var ind = que.requestQueue.indexOf(new LockRequest(txn.getTxnID(), null));
            Preconditions.checkState(ind != -1, "do not hold any lock when unlock.");


            // remove myself and notify all waiting transactions
            que.requestQueue.remove(ind);
            if (!que.requestQueue.isEmpty()) {
                que.cv.notifyAll();
            }

            txn.getSharedLockSet().remove(tpT);
            txn.getExclusiveLockSet().remove(tpT);

            // garbage collection
            // if no waiting transactions, this tp in this time point should be gc to avoid OOM.
            if (que.requestQueue.isEmpty()) {
                var l = lockTable.get(tp);
                Preconditions.checkNotNull(l);
                l.removeIf(new Predicate<Pair<Long, LockRequestQueue>>() {
                    @Override
                    public boolean apply(Pair<Long, LockRequestQueue> longLockRequestQueuePair) {
                        return Objects.equals(longLockRequestQueuePair.first(), pr.first());
                    }
                });
                if (l.isEmpty()) {
                    lockTable.remove(tp);
                }
            }
            return true;

        } finally {
            que.mu.unlock();
        }
    }

    public boolean unlock(TransactionImpl txn, long vertexID, String propertyName, long timestamp) throws TransactionAbortException {
        var tp = TemporalPropertyID.vertex(vertexID, propertyName);
        return doUnlock(txn, tp, timestamp);
    }

    public boolean unlock(TransactionImpl txn, long startID, long endID, String propertyName, long timestamp) throws TransactionAbortException {
        var tp = TemporalPropertyID.edge(startID, endID, propertyName);
        return doUnlock(txn, tp, timestamp);
    }

    // return true iff:
    // 1. que is empty
    // 2. compatible with all locks that are currently held
    // 3. does not exist any un-granted lock quest
    private static boolean isLockCompatible(LockRequestQueue que, LockRequest req) {
        Preconditions.checkNotNull(que);
        Preconditions.checkNotNull(req);
        for (var r : que.requestQueue) {
            // already apply for lock
            if (r.txnID == req.txnID) {
                return true;
            }
            // only test the compatibility with locks are currently held
            var compatible = r.granted &&
                    (r.lockMode != LockMode.EXCLUSIVE && req.lockMode != LockMode.EXCLUSIVE);
            if (!compatible) {
                return false;
            }
        }
        return true;
    }

    // throws abort exception to upper layer to let upper layer to release all locks
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
