package cn.edu.buaa.act.tgraph.dbms;

import cn.edu.buaa.act.tgraph.impl.dbms.DatabaseManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatabaseManagerTest {

    private static final Log log = LogFactory.getLog(DatabaseManagerTest.class);

    private static final String dbmsDir = "/Users/crusher/test/tgraph-dbms-test";

    private static final DatabaseManager dbms = DatabaseManager.of(dbmsDir);


    @Test
    void testSingleGraph() {
        dbms.createDatabase("test-graph");
        var graph = dbms.database("test-graph");

        log.info("graph start.");

        assertTrue(graph.isAvailable(1));

        dbms.shutdownDatabase("test-graph");
        dbms.dropDatabase("test-graph");
    }

    @Test
    void testMultipleGraphs() {

        for (int i = 0; i < 5; ++i) {
            String graphName = "test-graph" + i;
            dbms.createDatabase(graphName);
            var graph = dbms.database(graphName);
            assertTrue(graph.isAvailable(1));
        }

        // start, shut down, then restart, shut down, drop
        for (int i = 0; i < 5; ++i) {
            String graphName = "test-graph" + i;
            dbms.startDatabase(graphName);
        }
        for (int i = 0; i < 5; ++i) {
            String graphName = "test-graph" + i;
            dbms.shutdownDatabase(graphName);
        }
        for (int i = 0; i < 5; ++i) {
            String graphName = "test-graph" + i;
            dbms.startDatabase(graphName);
        }
        for (int i = 0; i < 5; ++i) {
            String graphName = "test-graph" + i;
            dbms.shutdownDatabase(graphName);
        }
        for (int i = 0; i < 5; ++i) {
            String graphName = "test-graph" + i;
            dbms.dropDatabase(graphName);
        }
    }
}
