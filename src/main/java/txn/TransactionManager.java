package txn;


import com.google.common.base.Preconditions;
import impl.tgraphdb.TGraphConfig;
import impl.tgraphdb.GraphSpaceID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import property.EdgeTemporalPropertyStore;
import property.VertexTemporalPropertyStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

// TransactionManager keeps track of all running transactions in this database.
// TransactionManager is in charge of concurrency control, failure recovery to guarantee ACID.
// Note!: All member variables are thread safe, do not require external synchronization


public class TransactionManager implements AutoCloseable {

    private final Log log = LogFactory.getLog(TransactionManager.class);

    private final AtomicLong nextTxnID = new AtomicLong(0);

    // store
    private final GraphDatabaseService neo;
    private final VertexTemporalPropertyStore vertex;
    private final EdgeTemporalPropertyStore edge;

    // transaction
    private final ConcurrentHashMap<Long, TransactionImpl> txnMap;
    private final LockManager lockManager;
    private final LogStore logStore;
    private final ActiveTransactionTable activeTxnTable;

    // background task executor
    ThreadPoolExecutor backgroundTaskExecutor = null;

    // purge
    final BlockingQueue<Long> purgeTransactions = new ArrayBlockingQueue<>(TGraphConfig.PURGE_BATCH_SIZE);
    final ScheduledExecutorService purgeThread = Executors.newSingleThreadScheduledExecutor();

    private static final int SHUTDOWN_TIME = 2;

    public TransactionManager(GraphSpaceID graph, GraphDatabaseService neo, VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge) {
        this.neo = neo;
        this.vertex = vertex;
        this.edge = edge;

        this.txnMap = new ConcurrentHashMap<>();
        this.lockManager = new LockManager(this.txnMap);
        this.logStore = new LogStore(graph, graph.getDatabasePath() + "/tp-redo-logs");
        this.activeTxnTable = new ActiveTransactionTable(graph, graph.getDatabasePath() + "/active-txn-table");
    }

    // start background task executor and purge task executor.
    public void start() {
        // Note!: you should custom your own thread pool instead of use Executors
        backgroundTaskExecutor = new ThreadPoolExecutor(TGraphConfig.BACKGROUND_THREAD_POOL_THREAD_NUMBER,
                TGraphConfig.BACKGROUND_THREAD_POOL_THREAD_NUMBER, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(TGraphConfig.MAX_CONCURRENT_TRANSACTION_NUMS),
                (r, executor) -> {
                    log.info("Too many concurrent transactions, apply this task in current thread.");
                    if (!executor.isShutdown()) {
                        r.run();
                    }
                });
        // background thread pool
        backgroundTaskExecutor.prestartAllCoreThreads();
        // purge thread
        purgeThread.scheduleAtFixedRate(this::purge, TGraphConfig.PURGE_INTERVAL, TGraphConfig.PURGE_INTERVAL, TimeUnit.SECONDS);
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public LogStore getLogStore() {
        return logStore;
    }

    public TransactionImpl getTransaction(long txnID) {
        return txnMap.get(txnID);
    }

    public TransactionImpl beginTransaction() {
        long txnID = nextTxnID.getAndIncrement();
        var txn = new TransactionImpl(txnID, neo.beginTx(), vertex, edge, this);
        // record txn state in persistent active table.
        activeTxnTable.put(txnID);
        // track this running transaction.
        txnMap.put(txn.getTxnID(), txn);
        return txn;
    }

    public void abortTransaction(TransactionImpl transaction) {
        if (transaction.getState() != TransactionState.ACTIVE) {
            return;
        }
        transaction.setState(TransactionState.ABORTED);
        Preconditions.checkNotNull(backgroundTaskExecutor, "you should start TransactionManager first.");
        // async for performance without safety sacrifice.
        backgroundTaskExecutor.submit(() -> asyncAbortTask(transaction));
    }

    private void asyncAbortTask(TransactionImpl txn) {
        // remove the active state in active table
        activeTxnTable.delete(txn.getTxnID());
        // release all locks.
        releaseLocks(txn);
        // stop tracking this running transaction
        txnMap.remove(txn.getTxnID());
    }

    // step 2 is the commit point.
    public void commitTransaction(TransactionImpl transaction) {
        if (transaction.getState() != TransactionState.ACTIVE) {
            return;
        }
        transaction.setState(TransactionState.COMMITTED);
        var txnID = transaction.getTxnID();
        // 1. write redo log
        logStore.commitBatchWrite(txnID, transaction.getLogWb());
        // 2. write commit log
        transaction.writeCommitLog();
        Preconditions.checkNotNull(backgroundTaskExecutor, "you should start TransactionManager first.");
        // async for performance without safety sacrifice.
        backgroundTaskExecutor.submit(() -> asyncCommitTask(transaction));
    }


    private void asyncCommitTask(TransactionImpl txn) {
        // 3. write temporal property store
        vertex.commitBatchWrite(txn.getVertexWb(), false, true, true);
        edge.commitBatchWrite(txn.getEdgeWb(), false, true, true);
        // 4. release lock
        releaseLocks(txn);
        try {
            purgeTransactions.put(txn.getTxnID());
        } catch (InterruptedException e) {
            log.info("Async commit task failed.");
            e.printStackTrace();
        }
        // 5. stop tracking this running transaction
        txnMap.remove(txn.getTxnID());
    }


    private void releaseLocks(TransactionImpl transaction) {
        try {
            lockManager.mu.lock();
            for (var s : transaction.getSharedLockSet()) {
                lockManager.unlock(transaction, s);
            }
            for (var x : transaction.getSharedLockSet()) {
                lockManager.unlock(transaction, x);
            }

        } finally {
            lockManager.mu.unlock();
        }
    }

    // used by EntityExecutor(Vertex, Edge) release locks when internal error(deadlock etc.) occur.
    public void releaseLocks(long txnID) {
        var txn = txnMap.get(txnID);
        Preconditions.checkNotNull(txn);
        releaseLocks(txn);
    }

    private void purgeCommitLog(List<Long> txnIDs) {
        try (var graphTxn = neo.beginTx()) {
            for (var txnID : txnIDs) {
                var commitNode = graphTxn.findNode(Label.label(TGraphConfig.COMMIT_LOG_NODE_LABEL), TGraphConfig.COMMIT_LOG_TXN_IDENTIFIER, txnID);
                if (commitNode != null) {
                    commitNode.delete();
                }
            }
            graphTxn.commit();
        }
    }

    private void purgeRedoLogAndActiveTable(List<Long> txnIDs) {
        // Note!: the order can not be changed
        // purge redo log first
        logStore.multiDelete(txnIDs);
        // purge active table latter
        activeTxnTable.multiDelete(txnIDs);
    }

    private void purge() {
        List<Long> txnIDs = new ArrayList<>();
        purgeTransactions.drainTo(txnIDs);
        // Note!: the order can not be changed
        purgeCommitLog(txnIDs);
        purgeRedoLogAndActiveTable(txnIDs);
    }

    private List<Long> getRedoTransactionList(List<Long> uncertain) {
        List<Long> redo = new ArrayList<>();
        try (var graphTxn = neo.beginTx()) {
            for (var txnID : uncertain) {
                if (graphTxn.findNode(Label.label(TGraphConfig.COMMIT_LOG_NODE_LABEL), TGraphConfig.COMMIT_LOG_TXN_IDENTIFIER, txnID) != null) {
                    redo.add(txnID);
                }
            }
            // you must commit this transaction even if this is a read only transaction.
            graphTxn.commit();
        }
        return redo;
    }

    private void redo(List<Long> redoList) {
        var entries = logStore.multiRead(redoList);
        var logApplier = new LogApplier(vertex, edge);
        logApplier.applyBatch(entries);
    }

    public void recover() {
        // 1. load uncertain transactions from active transaction table
        var mayActive = activeTxnTable.scan();
        // 2. judge those uncertain transactions through commit log
        var redoList = getRedoTransactionList(mayActive);
        // 3. redo those committed transactions.
        redo(redoList);
        // 4. purge all the garbage
        purgeCommitLog(mayActive);
        purgeRedoLogAndActiveTable(mayActive);
    }

    @Override
    public void close() throws InterruptedException {
        if (backgroundTaskExecutor != null) {
            // close background thread pool, purge thread, lock manager
            backgroundTaskExecutor.shutdown();
            if (!backgroundTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                log.info(String.format("Background thread pool did not terminate in %d second(s).", SHUTDOWN_TIME));
                backgroundTaskExecutor.shutdownNow();
            }
        }
        purgeThread.shutdown();
        if (!purgeThread.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
            log.info(String.format("purge thread did not terminate in %d second(s).", SHUTDOWN_TIME));
            purgeThread.shutdownNow();
        }
        lockManager.close();
        logStore.stop();
        activeTxnTable.stop();
        txnMap.clear();
    }
}
