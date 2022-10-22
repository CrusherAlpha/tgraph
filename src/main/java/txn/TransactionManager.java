package txn;


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
    // Note!: you should custom your own thread pool instead of use Executors
    ThreadPoolExecutor backgroundTaskExecutor = new ThreadPoolExecutor(TGraphConfig.BACKGROUND_THREAD_POOL_THREAD_NUMBER,
            TGraphConfig.BACKGROUND_THREAD_POOL_THREAD_NUMBER, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(TGraphConfig.MAX_CONCURRENT_TRANSACTION_NUMS),
            (r, executor) -> {
                log.info("Too many concurrent transactions, apply this task in current thread.");
                if (!executor.isShutdown()) {
                    r.run();
                }
            });

    // purge
    final BlockingQueue<Long> purgeTransactions = new ArrayBlockingQueue<>(TGraphConfig.PURGE_BATCH_SIZE);
    final ScheduledExecutorService purgeThread = Executors.newSingleThreadScheduledExecutor();

    public TransactionManager(GraphSpaceID graph, GraphDatabaseService neo, VertexTemporalPropertyStore vertex, EdgeTemporalPropertyStore edge) {
        this.neo = neo;
        this.vertex = vertex;
        this.edge = edge;

        this.txnMap = new ConcurrentHashMap<>();
        this.lockManager = new LockManager(this.txnMap);
        this.logStore = new LogStore(graph, graph.getDatabasePath() + "/log");
        this.activeTxnTable = new ActiveTransactionTable(graph, graph.getDatabasePath() + "/active-txn-table");
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
        transaction.setState(TransactionState.ABORTED);
        long txnID = transaction.getTxnID();
        // stop tracking this running transaction
        txnMap.remove(txnID);
        // remove the active state in active table
        activeTxnTable.delete(txnID);
    }

    // step 2 is the commit point.
    public void commitTransaction(TransactionImpl transaction) {
        transaction.setState(TransactionState.COMMITTED);
        var txnID = transaction.getTxnID();
        // 1. write redo log
        logStore.commitBatchWrite(txnID, transaction.getLogWb());
        // 2. write commit log && write topology store atomically through neo4j transaction
        transaction.writeCommitLog();
        transaction.commit();
        transaction.close();
        // async for performance without safety sacrifice.
        backgroundTaskExecutor.submit(() -> asyncCommitTask(transaction));
    }

    private void asyncCommitTask(TransactionImpl txn) {
        // 3. write temporal property store
        vertex.commitBatchWrite(txn.getVertexWb(), false, true, true);
        edge.commitBatchWrite(txn.getEdgeWb(), false, true, true);
        // 4. release lock
        releaseLock(txn);
        try {
            purgeTransactions.put(txn.getTxnID());
        } catch (InterruptedException e) {
            log.info("Async commit task failed.");
            e.printStackTrace();
        }
    }


    private void releaseLock(TransactionImpl transaction) {
        // TODO(crusher): impl it.
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
    public void close() throws Exception {
        // close background thread pool, purge thread, lock manager
    }
}
