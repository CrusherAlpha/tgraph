package cn.edu.buaa.act.tgraph.tgraphdb;

import cn.edu.buaa.act.tgraph.api.tgraphdb.Relationship;
import cn.edu.buaa.act.tgraph.api.tgraphdb.Transaction;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.GraphSpaceID;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.TGraphDatabase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import cn.edu.buaa.act.tgraph.txn.TransactionAbortException;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

enum TEST_REL_TYPE implements RelationshipType {
    TEST_REL
}

public class TGraphDatabaseTest {

    private static final Log log = LogFactory.getLog(TGraphDatabaseTest.class);


    private static final String databaseDir = "/Users/crusher/test/tgraph-test";
    private static final String graphDir = databaseDir + "/test-graph";
    private static final DatabaseManagementService neoDbms = new DatabaseManagementServiceBuilder(Path.of(graphDir)).build();
    private static final GraphDatabaseService neo = neoDbms.database(DEFAULT_DATABASE_NAME);

    @Test
    void testStart() {
        GraphSpaceID graph = new GraphSpaceID(1, "tg-db-test-start", graphDir);

        Instant start = Instant.now();
        var tg = new TGraphDatabase(graph, neo);
        Instant end = Instant.now();

        log.info(String.format("tg start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        assertEquals("tg-db-test-start", tg.databaseName());

        assertTrue(tg.isAvailable(5));

        tg.shutdown();
    }

    @Test
    void testTxnBase() {
        GraphSpaceID graph = new GraphSpaceID(1, "tg-db-test-txn-base", graphDir);

        Instant start = Instant.now();
        var tg = new TGraphDatabase(graph, neo);
        Instant end = Instant.now();

        log.info(String.format("tg start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));
        List<Transaction> txns = new ArrayList<>();

        for (int i = 0; i < 100; ++i) {
            txns.add(tg.beginTx());
        }

        for (int i = 0; i < 50; ++i) {
            txns.get(i).commit();
        }

        for (int i = 50; i < 100; ++i) {
            txns.get(i).rollback();
        }

        tg.shutdown();
    }

    @Test
    void testSingleTxnOps() {
        GraphSpaceID graph = new GraphSpaceID(1, "tg-db-test-single-txn-ops", graphDir);

        Instant start = Instant.now();
        var tg = new TGraphDatabase(graph, neo);
        Instant end = Instant.now();

        log.info(String.format("tg start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        try (var txn = tg.beginTx()) {
            var testLabel = Label.label("test-node");

            var node1 = txn.createNode(testLabel);
            node1.setProperty("name", "crusher");
            node1.createTemporalProperty("person_numbers");
            node1.setTemporalPropertyValue("person_numbers", Timestamp.from(Instant.now()), 100);
            TimeUnit.MILLISECONDS.sleep(100);
            node1.setTemporalPropertyValue("person_numbers", Timestamp.from(Instant.now()), 1000);

            var node2 = txn.createNode(testLabel);
            node2.setProperty("name", "alpha");
            node2.createTemporalProperty("person_numbers");
            node2.setTemporalPropertyValue("person_numbers", Timestamp.from(Instant.now()), 1500);

            var edge = node1.createRelationshipTo(node2, TEST_REL_TYPE.TEST_REL);
            assertEquals(node1, edge.getStartNode());
            assertEquals(node2, edge.getEndNode());
            edge.setProperty("name", "jack");
            edge.createTemporalProperty("hello");
            edge.setTemporalPropertyValue("hello", Timestamp.from(Instant.now()), 1000);

            txn.commit();
        } catch (TransactionAbortException e) {
            log.info("txn rollback.");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tg.shutdown();
    }

    @Test
    void testTxnRC() {
        String tpName = "person_numbers";

        Label testLabel = Label.label("test-node");

        GraphSpaceID graph = new GraphSpaceID(1, "tg-db-test-txn-rc", graphDir);

        Instant start = Instant.now();
        var tg = new TGraphDatabase(graph, neo);
        Instant end = Instant.now();

        log.info(String.format("tg start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        // copy from testSingleTxnOps
        // commit txn1 first.
        try (var txn = tg.beginTx()) {

            var node1 = txn.createNode(testLabel);
            log.info("txn1 node1 id: " + node1.getId());
            node1.setProperty("name", "crusher");
            node1.createTemporalProperty(tpName);
            node1.setTemporalPropertyValue(tpName, Timestamp.from(Instant.now()), 100);
            TimeUnit.MILLISECONDS.sleep(100);
            node1.setTemporalPropertyValue(tpName, Timestamp.from(Instant.now()), 1000);

            var node2 = txn.createNode(testLabel);
            log.info("txn1 node2 id: " + node2.getId());
            node2.setProperty("name", "alpha");
            node2.createTemporalProperty(tpName);
            node2.setTemporalPropertyValue(tpName, Timestamp.from(Instant.now()), 1500);

            var edge = node1.createRelationshipTo(node2, TEST_REL_TYPE.TEST_REL);
            log.info("txn1 edge id: " + edge.getId());
            assertEquals(node1, edge.getStartNode());
            assertEquals(node2, edge.getEndNode());
            edge.setProperty("name", "jack");
            edge.createTemporalProperty(tpName);
            edge.setTemporalPropertyValue(tpName, Timestamp.from(Instant.now()), 1000);

            txn.commit();
        } catch (TransactionAbortException e) {
            log.info("txn rollback.");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // then txn2 reads data.
        try (var txn = tg.beginTx()) {
            var node1 = txn.findNode(testLabel, "name", "crusher");
            var node2 = txn.findNode(testLabel, "name", "alpha");
            var edges = node1.getRelationships(TEST_REL_TYPE.TEST_REL);

            Relationship rel = null;

            int cnt = 0;
            for (var edge : edges) {
                ++cnt;
                rel = edge;
            }

            log.info("txn2 node1 id: " + node1.getId());
            log.info("txn2 node2 id: " + node2.getId());
            assertEquals(1, cnt);
            assertNotNull(rel);
            log.info("txn2 edge id: " + rel.getId());

            assertTrue(node1.hasTemporalProperty(tpName));
            assertEquals(1000, node1.getTemporalPropertyValue(tpName, Timestamp.from(Instant.now())));


            assertTrue(node2.hasTemporalProperty(tpName));
            assertEquals(1500, node2.getTemporalPropertyValue(tpName, Timestamp.from(Instant.now())));

            assertTrue(node1.hasRelationship(TEST_REL_TYPE.TEST_REL));
            assertEquals(1, node1.getDegree());

            assertEquals(node1, rel.getStartNode());
            assertEquals(node2, rel.getEndNode());
            assertEquals("jack", rel.getProperty("name"));
            assertEquals(1000, rel.getTemporalPropertyValue(tpName, Timestamp.from(Instant.now())));

            txn.commit();

        } catch (TransactionAbortException e) {
            log.info("txn rollback.");
            e.printStackTrace();
        }
        tg.shutdown();
    }

    @Test
    void testConcurrentRR() {
        String tpName = "person_numbers";

        Label testLabel = Label.label("test-node");

        GraphSpaceID graph = new GraphSpaceID(1, "tg-db-test-concurrent-rr", graphDir);

        Instant start = Instant.now();
        var tg = new TGraphDatabase(graph, neo);
        Instant end = Instant.now();

        log.info(String.format("tg start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        // commit one txn first.
        try (var txn = tg.beginTx()) {

            var node1 = txn.createNode(testLabel);
            log.info("txn1 node1 id: " + node1.getId());
            node1.setProperty("name", "crusher");
            node1.createTemporalProperty(tpName);
            node1.setTemporalPropertyValue(tpName, Timestamp.from(Instant.now()), 100);
            TimeUnit.MILLISECONDS.sleep(100);
            node1.setTemporalPropertyValue(tpName, Timestamp.from(Instant.now()), 1000);

            var node2 = txn.createNode(testLabel);
            log.info("txn1 node2 id: " + node2.getId());
            node2.setProperty("name", "alpha");
            node2.createTemporalProperty(tpName);
            node2.setTemporalPropertyValue(tpName, Timestamp.from(Instant.now()), 1500);

            var edge = node1.createRelationshipTo(node2, TEST_REL_TYPE.TEST_REL);
            log.info("txn1 edge id: " + edge.getId());
            assertEquals(node1, edge.getStartNode());
            assertEquals(node2, edge.getEndNode());
            edge.setProperty("name", "jack");
            edge.createTemporalProperty(tpName);
            edge.setTemporalPropertyValue(tpName, Timestamp.from(Instant.now()), 1000);

            txn.commit();
        } catch (TransactionAbortException e) {
            log.info("txn rollback.");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // start five concurrent read transactions
        List<Thread> ths = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            var t = new Thread(() -> {
                try (var txn = tg.beginTx()) {
                    var node1 = txn.findNode(testLabel, "name", "crusher");
                    var node2 = txn.findNode(testLabel, "name", "alpha");
                    var edges = node1.getRelationships(TEST_REL_TYPE.TEST_REL);

                    Relationship rel = null;

                    int cnt = 0;
                    for (var edge : edges) {
                        ++cnt;
                        rel = edge;
                    }

                    log.info("txn2 node1 id: " + node1.getId());
                    log.info("txn2 node2 id: " + node2.getId());
                    assertEquals(1, cnt);
                    assertNotNull(rel);
                    log.info("txn2 edge id: " + rel.getId());

                    assertTrue(node1.hasTemporalProperty(tpName));
                    assertEquals(1000, node1.getTemporalPropertyValue(tpName, Timestamp.from(Instant.now())));


                    assertTrue(node2.hasTemporalProperty(tpName));
                    assertEquals(1500, node2.getTemporalPropertyValue(tpName, Timestamp.from(Instant.now())));

                    assertTrue(node1.hasRelationship(TEST_REL_TYPE.TEST_REL));
                    assertEquals(1, node1.getDegree());

                    assertEquals(node1, rel.getStartNode());
                    assertEquals(node2, rel.getEndNode());
                    assertEquals("jack", rel.getProperty("name"));
                    assertEquals(1000, rel.getTemporalPropertyValue(tpName, Timestamp.from(Instant.now())));

                    txn.commit();

                } catch (TransactionAbortException e) {
                    log.info("txn rollback.");
                    e.printStackTrace();
                }
            });
            ths.add(t);
        }

        for (var th : ths) {
            th.start();
        }

        for (var th : ths) {
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



        tg.shutdown();

    }

    @Test
    void testConcurrentWW() {
        String tpName = "person_numbers";

        Label testLabel = Label.label("test-node");

        GraphSpaceID graph = new GraphSpaceID(1, "tg-db-test-concurrent-ww", graphDir);

        Instant start = Instant.now();
        var tg = new TGraphDatabase(graph, neo);
        Instant end = Instant.now();

        log.info(String.format("tg start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        var timestamp = Timestamp.from(Instant.now());

        // commit one txn first.
        try (var txn = tg.beginTx()) {

            var node = txn.createNode(testLabel);
            node.setProperty("name", "crusher");
            node.createTemporalProperty(tpName);
            node.setTemporalPropertyValue(tpName, timestamp, 0);
            txn.commit();
        } catch (TransactionAbortException e) {
            log.info("txn rollback.");
            e.printStackTrace();
        }

        // start five write transactions
        List<Thread> ths = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            int finalI = i;
            var t = new Thread(() -> {
                try (var txn = tg.beginTx()) {
                    var node = txn.findNode(testLabel, "name", "crusher");
                    var time = timestamp.getTime() + finalI + 1;
                    node.setTemporalPropertyValue(tpName, Timestamp.from(Instant.ofEpochMilli(time)), finalI + 1);
                    txn.commit();
                } catch (TransactionAbortException e) {
                    log.info("txn rollback.");
                    e.printStackTrace();
                }
            });
            ths.add(t);
        }

        for (var th : ths) {
            th.start();
        }

        for (var th : ths) {
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // test the final values.
        try (var txn = tg.beginTx()) {

            var node = txn.findNode(testLabel, "name", "crusher");
            assertNotNull(node);
            assertEquals(5, node.getTemporalPropertyValue(tpName, Timestamp.from(Instant.now())));
            txn.commit();
        } catch (TransactionAbortException e) {
            log.info("txn rollback.");
            e.printStackTrace();
        }

    }

    @Test
    void testConcurrentWR() {
        String tpName = "person_numbers";

        Label testLabel = Label.label("test-node");

        GraphSpaceID graph = new GraphSpaceID(1, "tg-db-test-concurrent-wr", graphDir);

        Instant start = Instant.now();
        var tg = new TGraphDatabase(graph, neo);
        Instant end = Instant.now();

        log.info(String.format("tg start elapsed time: %d millisecond(s).", Duration.between(start, end).toMillis()));

        var timestamp = Timestamp.from(Instant.now());

        // commit one txn first.
        try (var txn = tg.beginTx()) {

            var node = txn.createNode(testLabel);
            node.setProperty("name", "crusher");
            node.createTemporalProperty(tpName);
            node.setTemporalPropertyValue(tpName, timestamp, 0);
            txn.commit();
        } catch (TransactionAbortException e) {
            log.info("txn rollback.");
            e.printStackTrace();
        }

        // start ten concurrent write/read transactions.
        List<Thread> ths = new ArrayList<>();

        for (int i = 0; i < 10; ++i) {
            int finalI = i;
            var t = new Thread(() -> {
                if (finalI % 2 == 0) {
                    // write transactions
                    try (var txn = tg.beginTx()) {
                        var node = txn.findNode(testLabel, "name", "crusher");
                        try {
                            node.setTemporalPropertyValue(tpName, timestamp, 1);
                        } catch (TransactionAbortException e) {
                            log.info("txn rollback.");
                            e.printStackTrace();
                        }
                        txn.commit();
                    }
                } else {
                    // read transactions
                    try (var txn = tg.beginTx()) {
                        var node = txn.findNode(testLabel, "name", "crusher");
                        var val = (Integer) node.getTemporalPropertyValue(tpName, timestamp);
                        assertTrue(val == 0 || val == 1);
                        txn.commit();
                    } catch (TransactionAbortException e) {
                        log.info("txn rollback.");
                        e.printStackTrace();
                    }
                }
            });
            ths.add(t);
        }

        for (var th : ths) {
            th.start();
        }

        for (var th : ths) {
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
