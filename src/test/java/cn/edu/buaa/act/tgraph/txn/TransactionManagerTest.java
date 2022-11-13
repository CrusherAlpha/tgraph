package cn.edu.buaa.act.tgraph.txn;

import cn.edu.buaa.act.tgraph.impl.tgraphdb.GraphSpaceID;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.TGraphConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import cn.edu.buaa.act.tgraph.property.EdgeTemporalPropertyStore;
import cn.edu.buaa.act.tgraph.property.VertexTemporalPropertyStore;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TransactionManagerTest {
    private static final Log log = LogFactory.getLog(TransactionManager.class);

    private static final String databaseDir = "/Users/crusher/tgraph-test";
    private static final String graphDir = databaseDir + "/test-graph";
    private static final DatabaseManagementService neoDbms = new DatabaseManagementServiceBuilder(Path.of(graphDir)).build();
    private static final GraphDatabaseService neo = neoDbms.database(DEFAULT_DATABASE_NAME);

    @Test
    void testBasic() {
        Instant start = Instant.now();
        GraphSpaceID graph0 = new GraphSpaceID(1, "txn-manager-test-recover", graphDir);
        var vs = new VertexTemporalPropertyStore(graph0, graphDir + "/vertex-tp", false);
        var es = new EdgeTemporalPropertyStore(graph0, graphDir + "/edge-tp", false);

        Instant end = Instant.now();

        log.info(String.format("store create elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        start = Instant.now();
        var txnManager = new TransactionManager(graph0, neo, vs, es);
        txnManager.start();
        end = Instant.now();


        log.info(String.format("txn manager create and start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        List<TransactionImpl> txns = new ArrayList<>();

        start = Instant.now();
        for (int i = 0; i < 10; ++i) {
            txns.add(txnManager.beginTransaction());
        }
        end = Instant.now();

        log.info(String.format("create 10 transactions elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));
        assertEquals(10, txns.size());

        log.info("after begin 10 txns.");

        start = Instant.now();
        for (int i = 0; i < 5; ++i) {
            txnManager.abortTransaction(txns.get(i));
        }
        end = Instant.now();

        log.info(String.format("abort 5 txns elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        log.info("after abort top 5 txns.");

        start = Instant.now();
        for (int i = 5; i < 10; ++i) {
            txnManager.commitTransaction(txns.get(i));
        }
        end = Instant.now();

        log.info(String.format("commit 5 txns elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));
        log.info("after commit tail 5 txns.");

        try {
            txnManager.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        log.info("after txn manager close.");

    }

    @Test
    void testRecover() {
        Instant start = Instant.now();
        GraphSpaceID graph0 = new GraphSpaceID(1, "txn-manager-test-redo", graphDir);
        var vs = new VertexTemporalPropertyStore(graph0, graphDir + "/vertex-tp", false);
        var es = new EdgeTemporalPropertyStore(graph0, graphDir + "/edge-tp", false);

        Instant end = Instant.now();

        log.info(String.format("store create elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        start = Instant.now();
        var txnManager = new TransactionManager(graph0, neo, vs, es);
        end = Instant.now();
        log.info(String.format("txn manager create elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        start = Instant.now();
        txnManager.recover();
        end = Instant.now();

        log.info(String.format("txn manager recover elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        txnManager.start();

        // copy from testBasic
        List<TransactionImpl> txns = new ArrayList<>();
        start = Instant.now();
        for (int i = 0; i < 10; ++i) {
            txns.add(txnManager.beginTransaction());
        }
        end = Instant.now();

        log.info(String.format("create 10 transactions elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));
        assertEquals(10, txns.size());

        log.info("after begin 10 txns.");

        start = Instant.now();
        for (int i = 0; i < 5; ++i) {
            txnManager.abortTransaction(txns.get(i));
        }
        end = Instant.now();

        log.info(String.format("abort 5 txns elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        log.info("after abort top 5 txns.");

        start = Instant.now();
        for (int i = 5; i < 10; ++i) {
            txnManager.commitTransaction(txns.get(i));
        }
        end = Instant.now();

        log.info(String.format("commit 5 txns elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));
        log.info("after commit tail 5 txns.");

        try {
            txnManager.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        log.info("after txn manager close.");

    }


    @Test
    void testRecoverMore() {
        Instant start = Instant.now();
        GraphSpaceID graph0 = new GraphSpaceID(1, "txn-manager-test-recover-more", graphDir);
        var vs = new VertexTemporalPropertyStore(graph0, graphDir + "/vertex-tp", false);
        var es = new EdgeTemporalPropertyStore(graph0, graphDir + "/edge-tp", false);

        Instant end = Instant.now();

        log.info(String.format("store create elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        start = Instant.now();
        var txnManager = new TransactionManager(graph0, neo, vs, es);
        txnManager.start();
        end = Instant.now();

        log.info(String.format("txn manager create and start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        List<TransactionImpl> txns = new ArrayList<>();

        start = Instant.now();
        for (int i = 0; i < 100; ++i) {
            txns.add(txnManager.beginTransaction());
        }
        end = Instant.now();

        log.info(String.format("create 100 transactions elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));


        start = Instant.now();
        for (int i = 0; i < 50; ++i) {
            txnManager.abortTransaction(txns.get(i));
        }
        end = Instant.now();

        log.info(String.format("abort 50 txns elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));


        start = Instant.now();
        for (int i = 50; i < 100; ++i) {
            txnManager.commitTransaction(txns.get(i));
        }
        end = Instant.now();

        log.info(String.format("commit 50 txns elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        try {
            // wait purge
            TimeUnit.SECONDS.sleep(TGraphConfig.PURGE_INTERVAL * 2L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            txnManager.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // test recover, observe the state.
        txnManager = new TransactionManager(graph0, neo, vs, es);
        txnManager.recover();


    }


}
