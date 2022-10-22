package impl.dbms;

import api.dbms.DatabaseManagementService;
import api.tgraphdb.TGraphDatabaseService;
import com.google.common.base.Preconditions;
import impl.tgraphdb.GraphSpaceID;
import impl.tgraphdb.TGraphDatabase;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.api.DatabaseNotFoundException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

// unique globally.
// Note!: not thread safe.
public class DatabaseManager implements DatabaseManagementService {

    private final MetaDB metaDB;

    private final String metaPath;
    private final String tGraphPath;

    private HashMap<String, TGraphDatabaseService> openedTG = new HashMap<>();
    private HashMap<String, org.neo4j.dbms.api.DatabaseManagementService> opendNeo4jDBMS = new HashMap<>();

    public DatabaseManager(String databaseDirectory) {
        this.metaPath = databaseDirectory + "/meta";
        this.tGraphPath = databaseDirectory + "/db";
        this.metaDB = new MetaDB(this.metaPath);
    }

    @Override
    public TGraphDatabaseService database(String databaseName) throws DatabaseNotFoundException {
        if(openedTG.containsKey(databaseName)) {
            var ret = openedTG.get(databaseName);
            Preconditions.checkNotNull(ret);
            return ret;
        }
        var id = metaDB.get(databaseName);
        if (id == null) {
            throw new DatabaseNotFoundException();
        }
        var dbms = opendNeo4jDBMS.get(databaseName);
        Preconditions.checkNotNull(dbms);
        return new TGraphDatabase(new GraphSpaceID(id, databaseName, tGraphPath + "/" + databaseName), dbms.database(DEFAULT_DATABASE_NAME));
    }

    @Override
    public void createDatabase(String databaseName) throws DatabaseExistsException {
        if (openedTG.containsKey(databaseName)) {
            throw new DatabaseExistsException();
        }
        var id = metaDB.get(databaseName);
        if (id != null) {
            throw new DatabaseExistsException();
        }
        var dbms = new DatabaseManagementServiceBuilder(Path.of(tGraphPath + "/" + databaseName)).build();
        opendNeo4jDBMS.put(databaseName, dbms);
        var tg = new TGraphDatabase(new GraphSpaceID(1, databaseName, tGraphPath + "/" + databaseName), dbms.database(DEFAULT_DATABASE_NAME));
        openedTG.put(databaseName, tg);
        metaDB.put(databaseName, 1);
    }

    @Override
    public void dropDatabase(String databaseName) throws DatabaseNotFoundException {
        if (metaDB.get(databaseName) == null) {
            throw new DatabaseNotFoundException();
        }
        if (openedTG.containsKey(databaseName)) {
            var tg = openedTG.get(databaseName);
            tg.shutdown();
            openedTG.remove(databaseName);
        }
        if (opendNeo4jDBMS.containsKey(databaseName)) {
            var dbms = opendNeo4jDBMS.get(databaseName);
            dbms.shutdown();
            opendNeo4jDBMS.remove(databaseName);
        }
        metaDB.delete(databaseName);
    }

    @Override
    public void startDatabase(String databaseName) throws DatabaseNotFoundException {
        if (openedTG.containsKey(databaseName)) {
            // already start, just return
            Preconditions.checkState(opendNeo4jDBMS.containsKey(databaseName));
            return;
        }
        var id = metaDB.get(databaseName);
        if (id == null) {
            throw new DatabaseNotFoundException();
        }
        var dbms = new DatabaseManagementServiceBuilder(Path.of(tGraphPath + "/" + databaseName)).build();
        opendNeo4jDBMS.put(databaseName, dbms);
        var tg = new TGraphDatabase(new GraphSpaceID(id, databaseName, tGraphPath + "/" + databaseName), dbms.database(DEFAULT_DATABASE_NAME));
        openedTG.put(databaseName, tg);
    }

    @Override
    public void shutdownDatabase(String databaseName) throws DatabaseNotFoundException {
        if (metaDB.get(databaseName) == null) {
            throw new DatabaseNotFoundException();
        }
        if (!openedTG.containsKey(databaseName)) {
            // already shutdown, just return
            Preconditions.checkState(!opendNeo4jDBMS.containsKey(databaseName));
            return;
        }
        var dbms = opendNeo4jDBMS.get(databaseName);
        var tg = openedTG.get(databaseName);
        tg.shutdown();
        dbms.shutdown();
        opendNeo4jDBMS.remove(databaseName);
        openedTG.remove(databaseName);
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
        for (var entry : opendNeo4jDBMS.values()) {
            entry.shutdown();
        }
        opendNeo4jDBMS.clear();
    }
}
