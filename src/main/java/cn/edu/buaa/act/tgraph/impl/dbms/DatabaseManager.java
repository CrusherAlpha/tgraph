package cn.edu.buaa.act.tgraph.impl.dbms;

import cn.edu.buaa.act.tgraph.api.dbms.DatabaseManagementService;
import cn.edu.buaa.act.tgraph.api.tgraphdb.TGraphDatabaseService;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.TGraphDatabase;
import com.google.common.base.Preconditions;
import cn.edu.buaa.act.tgraph.impl.tgraphdb.GraphSpaceID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.api.DatabaseNotFoundException;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

// unique globally.
// NOTE!: not thread safe.
public class DatabaseManager implements DatabaseManagementService {

    private static final Log log = LogFactory.getLog(DatabaseManager.class);

    private final MetaDB metaDB;

    private final String tGraphPath;

    // NOTE!: this two maps must be consistent.
    // started graph will be put into this two maps.
    private final HashMap<String, TGraphDatabaseService> openedTG = new HashMap<>();
    private final HashMap<String, org.neo4j.dbms.api.DatabaseManagementService> openedNeo4jDBMS = new HashMap<>();

    // NOTE!: invariant: openedTG.containsKey(graph) && !shutdownTG.containsKey(graph)
    private final HashMap<String, TGraphDatabaseService> shutdownTG = new HashMap<>();


    private DatabaseManager(String databaseDirectory) {
        String metaPath = databaseDirectory + "/meta";
        this.tGraphPath = databaseDirectory + "/db";
        this.metaDB = new MetaDB(metaPath);
    }

    public static DatabaseManager of(String databaseDirectory) {
        return new DatabaseManager(databaseDirectory);
    }

    private TGraphDatabaseService doStartDatabase(String databaseName) {
        shutdownTG.remove(databaseName);
        String graphPath = tGraphPath + "/" + databaseName;
        var dbms = new DatabaseManagementServiceBuilder(Path.of(graphPath)).build();
        openedNeo4jDBMS.put(databaseName, dbms);
        var graph = dbms.database(DEFAULT_DATABASE_NAME);
        var tg = new TGraphDatabase(new GraphSpaceID(1, databaseName, graphPath), graph);
        openedTG.put(databaseName, tg);
        return tg;
    }

    @Override
    public TGraphDatabaseService database(String databaseName) throws DatabaseNotFoundException {
        // if started already, just return it
        if(openedTG.containsKey(databaseName)) {
            var ret = openedTG.get(databaseName);
            Preconditions.checkNotNull(ret);
            return ret;
        }
        var id = metaDB.get(databaseName);
        if (id == null) {
            throw new DatabaseNotFoundException();
        }
        var dbms = openedNeo4jDBMS.get(databaseName);
        Preconditions.checkState(dbms == null);

        // or, start it and return
        return doStartDatabase(databaseName);
    }

    @Override
    public void createDatabase(String databaseName) throws DatabaseExistsException {
        // if started, graph exists
        if (openedTG.containsKey(databaseName)) {
            throw new DatabaseExistsException();
        }
        var id = metaDB.get(databaseName);
        if (id != null) {
            throw new DatabaseExistsException();
        }
        metaDB.put(databaseName, 1);
    }

    private boolean deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                deleteDirectory(file);
            }
        }
        return dir.delete();
    }

    @Override
    public void dropDatabase(String databaseName) throws DatabaseNotFoundException {
        if (metaDB.get(databaseName) == null) {
            throw new DatabaseNotFoundException();
        }
        if (!shutdownTG.containsKey(databaseName)) {
            log.error("You should shut down database first.");
            return;
        }
        var tg = shutdownTG.get(databaseName);
        tg.drop();
        String graphPath = tGraphPath + "/" + databaseName;
        // neo4j does not support delete default graph, we just call the filesystem api to do it.
        Preconditions.checkState(deleteDirectory(new File(graphPath)));
        shutdownTG.remove(databaseName);
        metaDB.delete(databaseName);
    }

    @Override
    public void startDatabase(String databaseName) throws DatabaseNotFoundException {
        if (openedTG.containsKey(databaseName)) {
            // already start, just return
            Preconditions.checkState(openedNeo4jDBMS.containsKey(databaseName));
            Preconditions.checkState(!shutdownTG.containsKey(databaseName));
            return;
        }
        var id = metaDB.get(databaseName);
        if (id == null) {
            throw new DatabaseNotFoundException();
        }
        doStartDatabase(databaseName);
    }

    @Override
    public void shutdownDatabase(String databaseName) throws DatabaseNotFoundException {
        if (metaDB.get(databaseName) == null) {
            throw new DatabaseNotFoundException();
        }
        if (shutdownTG.containsKey(databaseName)) {
            // already shutdown, just return
            Preconditions.checkState(!openedNeo4jDBMS.containsKey(databaseName));
            Preconditions.checkState(!openedTG.containsKey(databaseName));
            return;
        }
        var dbms = openedNeo4jDBMS.get(databaseName);
        var tg = openedTG.get(databaseName);
        tg.shutdown();
        dbms.shutdown();
        openedNeo4jDBMS.remove(databaseName);
        openedTG.remove(databaseName);
        shutdownTG.put(databaseName, tg);
    }

    @Override
    public List<String> listDatabases() {
        var all = metaDB.scan();
        List<String> ret = new ArrayList<>();
        for (var pr : all) {
            ret.add(pr.first());
        }
        return ret;
    }

    @Override
    public void shutdown() {
        for (var entry : openedTG.values()) {
            entry.shutdown();
        }
        openedTG.clear();
        for (var entry : openedNeo4jDBMS.values()) {
            entry.shutdown();
        }
        openedNeo4jDBMS.clear();
        shutdownTG.clear();
    }
}
